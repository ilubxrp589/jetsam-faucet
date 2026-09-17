// Jetsam faucet backend.
//
// Model: claims QUEUE rather than dispense instantly. Once BATCH_SIZE
// distinct addresses are queued, the whole batch fires as BATCH_SIZE
// separate walletSend calls (Jetsam transactions cap at 2 outputs each,
// so there is no single multi-recipient transaction to build here).
// Batching throttles abuse (no instant drain on request #1) and gives the
// status bar something real to show.
//
// This process needs loopback RPC access to a Jetsam node with a FUNDED,
// UNLOCKED wallet loaded — deliberately a small dedicated float, never the
// mining payout wallet. Run this on the same host as that node so the
// RPC call never leaves loopback and never crosses the browser-origin
// block the node enforces on purpose.
"use strict";

import { createServer } from "node:http";
import { readFile, writeFile, rename } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

const CONFIG = {
  rpcUrl: process.env.FAUCET_RPC || "http://127.0.0.1:9701",
  port: Number(process.env.FAUCET_PORT || 3790),
  batchSize: Number(process.env.FAUCET_BATCH_SIZE || 10),
  // Two claim modes. Instant costs the faucet the same relay fee for a
  // smaller payout, so it deliberately pays less; waiting for a batch pays
  // more. Batching does NOT save fees here — Jetsam txs cap at 2 outputs, so
  // ten recipients is always ten transactions and ten fees.
  amountSingleMicroJtm: Math.round(Number(process.env.FAUCET_AMOUNT_SINGLE_JTM || 0.004) * 1_000_000),
  amountBatchMicroJtm: Math.round(Number(process.env.FAUCET_AMOUNT_BATCH_JTM || 0.01) * 1_000_000),
  cooldownHours: Number(process.env.FAUCET_COOLDOWN_HOURS || 24),
  // A batch that never fills would strand whoever joined it. Measured from
  // the OLDEST entry, so the first person to join sets the deadline and
  // nobody waits longer than this no matter how quiet it is.
  batchMaxAgeMs: Number(process.env.FAUCET_BATCH_MAX_AGE_HOURS || 24) * 3600 * 1000,
  // Minimum gap between DEPARTURES. Without it, back-to-back full batches
  // chain with zero gap: 10 sends each at one block apiece is 10-33 min of
  // continuous wallet lock, and every instant claim in that window is refused
  // ("another payment is going out right now"). The gap buys the instant lane
  // guaranteed clear windows. Age-out departures are exempt -- those people
  // already waited a full cycle.
  batchMinGapMs: Number(process.env.FAUCET_BATCH_MIN_GAP_MINUTES || 30) * 60 * 1000,
  // How long to wait for the previous send's change to confirm before giving
  // up on one recipient and leaving them queued. Blocks are ~90s.
  spendWaitMs: Number(process.env.FAUCET_SPEND_WAIT_MS || 240_000),
  minFloatMicroJtm: Math.round(Number(process.env.FAUCET_MIN_FLOAT_JTM || 20) * 1_000_000),
  // Per-send relay fee the sender pays on top of the payout. Measured at
  // 9,000 uJTM; a little headroom so a fee bump doesn't strand a batch.
  feeAllowanceMicroJtm: Number(process.env.FAUCET_FEE_ALLOWANCE_MICRO || 10_000),
  dataFile: process.env.FAUCET_DATA_FILE || path.join(import.meta.dirname, "faucet-data.json"),
  // Loopback by default. The deployment on .220 sets this to the LAN address
  // so Caddy on .39 can reach it; the node's wallet RPC still never leaves
  // loopback on the faucet host.
  bind: process.env.FAUCET_BIND || "127.0.0.1",
  // Comma-separated allow-list for the status/claim page's own origin. The
  // page and this API are meant to be same-site via a Caddy /api/* proxy,
  // this is a second, explicit layer, not the only one.
  allowedOrigin: process.env.FAUCET_ALLOWED_ORIGIN || "https://jtmfaucet.halcyon-names.io",
};

// ---- bech32m address validation (Jetsam HRP "j"), same algorithm as the ---
// ---- page's client-side validator, kept server-side as the real gate. ----
const CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";
const BECH32M_CONST = 0x2bc830a3;
const HRP = "j";
function polymod(values) {
  const GEN = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3];
  let chk = 1;
  for (const v of values) {
    const top = chk >>> 25;
    chk = ((chk & 0x1ffffff) << 5) ^ v;
    for (let i = 0; i < 5; i++) if ((top >>> i) & 1) chk ^= GEN[i];
  }
  return chk >>> 0;
}
function hrpExpand(hrp) {
  const ret = [];
  for (const c of hrp) ret.push(c.charCodeAt(0) >> 5);
  ret.push(0);
  for (const c of hrp) ret.push(c.charCodeAt(0) & 31);
  return ret;
}
function validateAddress(addr) {
  if (typeof addr !== "string" || !addr) return false;
  if (addr !== addr.toLowerCase() && addr !== addr.toUpperCase()) return false;
  const s = addr.toLowerCase();
  const pos = s.lastIndexOf("1");
  if (pos < 1 || pos + 7 > s.length) return false;
  if (s.slice(0, pos) !== HRP) return false;
  const data = [];
  for (const ch of s.slice(pos + 1)) {
    const v = CHARSET.indexOf(ch);
    if (v === -1) return false;
    data.push(v);
  }
  return polymod(hrpExpand(HRP).concat(data)) === BECH32M_CONST;
}

// ---- persistent state (atomic write: tmp file + rename) -------------------
const EMPTY_STATE = {
  // The batch clock runs CONTINUOUSLY, independent of the queue. At each
  // expiry whoever is queued gets paid; if nobody is, nothing is sent and the
  // clock simply rolls forward. Persisted so a restart does not silently grant
  // everyone a fresh 24h wait.
  nextFlushAt: 0,     // epoch ms of the next send opportunity
  lastDepartureAt: 0, // epoch ms the last batch finished sending
  queue: [],          // [{address, ipHash, queuedAt}]
  recentByAddress: {}, // "<mode>:<address>" -> last claimed-or-queued epoch ms
  recentByIp: {},      // "<mode>:<ipHash>"  -> last claimed-or-queued epoch ms
  history: [],          // [{batchId, completedAt, sends:[{address,txid,amountMicroJtm,ok,error?}]}]
  totalDispensedMicroJtm: 0,
  totalClaims: 0,
  nextBatchId: 1,
  paused: false,
  pausedReason: "",
};
let state = EMPTY_STATE;
let saving = Promise.resolve();

async function loadState() {
  if (existsSync(CONFIG.dataFile)) {
    try {
      state = { ...EMPTY_STATE, ...JSON.parse(await readFile(CONFIG.dataFile, "utf8")) };
    } catch (e) {
      console.error("faucet-data.json unreadable, starting fresh:", e.message);
    }
  }
}
function saveState() {
  // Serialize writes so a batch-processing save can't race a claim's save.
  saving = saving.then(async () => {
    const tmp = CONFIG.dataFile + ".tmp";
    await writeFile(tmp, JSON.stringify(state, null, 2));
    await rename(tmp, CONFIG.dataFile);
  });
  return saving;
}

// Cooldown records are only meaningful for one window. An expired one already
// blocks nobody — the check is `now - last < cooldown` — but nothing ever
// removed them, so the file grew forever and saveState rewrites the whole
// thing on every claim. Pruning keeps the state proportional to live users
// rather than to all users ever.
function pruneExpired() {
  const cutoff = Date.now() - cooldownActiveMs();
  let removed = 0;
  for (const map of [state.recentByAddress, state.recentByIp]) {
    for (const key of Object.keys(map)) {
      if (map[key] < cutoff) {
        delete map[key];
        removed++;
      }
    }
  }
  return removed;
}

function hashIp(ip) {
  return crypto.createHash("sha256").update(ip).digest("hex").slice(0, 16);
}
function cooldownActiveMs() {
  return CONFIG.cooldownHours * 3600 * 1000;
}

// ---- RPC ------------------------------------------------------------------
let rpcId = 0;
// The node's RPC trait is declared `#[rpc(server, namespace = "jetsam")]`,
// so every method is prefixed on the wire. Calling the bare name returns
// -32601 Method not found, which would look like a dead node rather than a
// typo. jetsam-cli does the same prefixing internally.
async function rpcCall(method, params) {
  const res = await fetch(CONFIG.rpcUrl, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: ++rpcId, method: `jetsam_${method}`, params }),
    // An instant claim sends inline, so a wedged node would otherwise hang
    // the claimer's HTTP request forever.
    signal: AbortSignal.timeout(Number(process.env.FAUCET_RPC_TIMEOUT_MS || 120_000)),
  });
  const body = await res.json();
  if (body.error) throw new Error(`${method}: ${body.error.message || JSON.stringify(body.error)}`);
  return body.result;
}

async function currentFloatMicroJtm() {
  const bal = await rpcCall("walletGetBalance", []);
  return bal.balance_micro_jtm;
}

// Cached float. Checked BEFORE accepting a claim, not only after a batch:
// an unfunded or drained wallet would otherwise accept a full batch of
// claims and then fail every send, burning each claimer's cooldown for
// nothing. Cached so a burst of claims doesn't hammer the node's RPC.
const FLOAT_TTL_MS = 30_000;
let floatCache = { value: null, at: 0 };

// `walletGetBalance.balance_micro_jtm` counts CONFIRMED state only. The wallet
// holds a single UTXO, so a send locks the whole input and the node keeps
// reporting the OLD balance until the change confirms (~a block). The claim
// response already returns the correct projected figure, but the page's 5s
// poll then overwrote it with that stale balance and the number snapped back
// up -- which reads as "the balance never updated" (reported 2026-09-14).
//
// So: after a send, hold the projected value as a FLOOR. Report it until the
// node's own balance drops to it (confirmation), then let the node lead again.
// Applied to the funding pre-flight too, where erring low is the safe side.
let pendingFloor = { value: null, at: 0 };
const PENDING_FLOOR_MAX_MS = 30 * 60_000; // never hold a stale floor forever

function notePendingSpend(projectedMicro) {
  if (projectedMicro == null) return;
  pendingFloor = {
    // Two sends before either confirms: keep the lower projection.
    value: pendingFloor.value == null ? projectedMicro : Math.min(pendingFloor.value, projectedMicro),
    at: Date.now(),
  };
}

function applyPendingFloor(nodeValue) {
  if (nodeValue == null || pendingFloor.value === null) return nodeValue;
  if (Date.now() - pendingFloor.at > PENDING_FLOOR_MAX_MS) {
    pendingFloor = { value: null, at: 0 };   // stuck/dropped tx: trust the node
    return nodeValue;
  }
  if (nodeValue <= pendingFloor.value) {
    pendingFloor = { value: null, at: 0 };   // confirmed: the node has caught up
    return nodeValue;
  }
  return pendingFloor.value;
}

async function cachedFloatMicroJtm() {
  const now = Date.now();
  if (floatCache.value !== null && now - floatCache.at < FLOAT_TTL_MS) return applyPendingFloor(floatCache.value);
  try {
    const v = await currentFloatMicroJtm();
    floatCache = { value: v, at: now };
    return applyPendingFloor(v);
  } catch (e) {
    // A node that cannot be reached is not the same as an empty wallet.
    // Report null and let the caller refuse the claim with an honest reason
    // rather than silently treating it as zero.
    return null;
  }
}
// The wallet holds few UTXOs — often exactly one. Spending locks the whole
// input until the change confirms, so `spendable` drops to zero for a block
// (~90s) after every send and the next one fails with InsufficientFunds.
// Sends must therefore be serialised, one per block, not fired in a loop.
async function waitForSpendable(needMicro, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      const bal = await rpcCall("walletGetBalance", []);
      if ((bal.spendable_micro_jtm ?? 0) >= needMicro) return true;
    } catch (e) {
      // retry; a transient RPC error is not a funding answer
    }
    if (Date.now() >= deadline) return false;
    await new Promise((r) => setTimeout(r, 10_000));
  }
}

// A batch costs the payouts PLUS one relay fee per send, and the sender pays
// the fee on top of the amount. Measured min relay fee on this chain is
// 9,000 uJTM for a 1-in/2-out send, which at small payouts is a large share
// of the total — ignoring it lets the wallet accept a full batch it can only
// partly pay.
function batchCostMicroJtm() {
  return CONFIG.batchSize * (CONFIG.amountBatchMicroJtm + CONFIG.feeAllowanceMicroJtm);
}

// Exactly one send may be in flight across the whole process. The wallet can
// only fund one transaction per block, and the node proves transactions in a
// single slot, so letting an instant claim and a batch send race just produces
// two failures and two proving jobs instead of one payment.
//
// Claimed SYNCHRONOUSLY. Checking the flag and then awaiting anything before
// setting it leaves a window where every concurrent request sees it clear —
// which is exactly how a 12-way burst produced 11 attempted sends instead of
// 11 clean refusals.
let sending = false;
function tryAcquireSend() {
  if (sending) return false;
  sending = true;
  return true;
}
async function acquireSendWithin(timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (tryAcquireSend()) return true;
    if (Date.now() >= deadline) return false;
    await new Promise((r) => setTimeout(r, 500));
  }
}

// ---- batch processing -------------------------------------------------
let processing = false;
// At most ONE pending held-departure timer: every claim that arrives while a
// full batch is held would otherwise schedule its own, firing a burst of
// redundant maybeProcessBatch calls the instant the gap expires.
let heldTimer = null;
function scheduleHeldDeparture(ms) {
  if (heldTimer) return;
  heldTimer = setTimeout(() => { heldTimer = null; maybeProcessBatch(); }, Math.max(1000, ms));
  if (heldTimer.unref) heldTimer.unref();
}
function heldUntil() {
  if (state.queue.length < CONFIG.batchSize) return null;   // not full: not held
  const at = (state.lastDepartureAt || 0) + CONFIG.batchMinGapMs;
  return at > Date.now() ? at : null;
}
async function maybeProcessBatch(force = false) {
  if (processing) return;
  if (state.queue.length === 0) return;
  const full = state.queue.length >= CONFIG.batchSize;
  // `force` is the age-out path: send a short batch rather than leave people
  // queued indefinitely on a quiet day.
  if (!full && !force) return;
  // A full batch still waits out the minimum gap since the last departure, so
  // a rush of claims cannot monopolise the wallet and starve the instant lane.
  if (full && !force) {
    const since = Date.now() - (state.lastDepartureAt || 0);
    if (since < CONFIG.batchMinGapMs) {
      scheduleHeldDeparture(CONFIG.batchMinGapMs - since);
      return;
    }
  }
  const take = full ? CONFIG.batchSize : state.queue.length;
  processing = true;
  try {
    const batch = state.queue.slice(0, take);
    const sends = [];
    const unpaid = [];   // anyone we cannot pay stays queued, never dropped
    for (const entry of batch) {
      // Pay what was promised when they queued, not whatever the config
      // says now, so a mid-queue settings change cannot short anyone.
      const owed = entry.amountMicroJtm ?? CONFIG.amountBatchMicroJtm;
      const need = owed + CONFIG.feeAllowanceMicroJtm;

      // Block until the PREVIOUS send's change has confirmed. Without this
      // the second and every later send in a batch fails outright, because
      // spending the single UTXO locks the whole balance for a block.
      if (!(await waitForSpendable(need, CONFIG.spendWaitMs))) {
        console.error(`no spendable funds for ${entry.address} after ${CONFIG.spendWaitMs}ms — leaving them queued`);
        unpaid.push(entry);
        continue;
      }
      if (!(await acquireSendWithin(CONFIG.spendWaitMs))) {
        console.error(`could not take the send lock for ${entry.address} — leaving them queued`);
        unpaid.push(entry);
        continue;
      }
      try {
        const result = await rpcCall("walletSend", [entry.address, owed, 0]);
        sends.push({ address: entry.address, txid: result.txid, amountMicroJtm: result.amount_micro_jtm, ok: true });
        state.totalDispensedMicroJtm += result.amount_micro_jtm;
      } catch (e) {
        // Requeued rather than written off: a failed send must not cost
        // someone their place in line or their daily claim.
        sends.push({ address: entry.address, ok: false, error: String(e.message || e) });
        console.error("faucet send failed for", entry.address, e);
        unpaid.push(entry);
      } finally {
        sending = false;
      }
    }
    // Unpaid entries return to the FRONT so they keep their place.
    state.queue = [...unpaid, ...state.queue.slice(take)];
    state.history.unshift({
      batchId: state.nextBatchId++,
      completedAt: Date.now(),
      partial: take < CONFIG.batchSize,
      sends,
    });
    state.history = state.history.slice(0, 50); // bounded history
    // A send just happened (full batch, or the age-out path): start a fresh
    // window from now so the clock always reads "time until the next send".
    state.nextFlushAt = Date.now() + CONFIG.batchMaxAgeMs;
    state.lastDepartureAt = Date.now();
    await saveState();

    // Auto-pause if the float can no longer cover another full batch.
    // Invalidate the cache first: the balance just dropped by a whole batch.
    floatCache = { value: null, at: 0 };
    const float = await currentFloatMicroJtm().catch(() => null);
    if (float !== null) floatCache = { value: float, at: Date.now() };
    // waitForSpendable serialises the batch, so every send but the LAST has
    // already confirmed and is reflected above; only the final one is pending.
    const lastOk = [...sends].reverse().find((x) => x.ok);
    if (float !== null && lastOk)
      notePendingSpend(Math.max(0, float - (lastOk.amountMicroJtm || 0) - CONFIG.feeAllowanceMicroJtm));
    if (float !== null && float < CONFIG.minFloatMicroJtm) {
      state.paused = true;
      state.pausedReason = "float too low for the next batch";
      await saveState();
    }
  } finally {
    processing = false;
    // A batch may have left another full batch already queued (a burst of
    // claims arrived while this one processed) — chain immediately.
    // Chain the next full batch, but through the gap rather than immediately.
    if (state.queue.length >= CONFIG.batchSize) scheduleHeldDeparture(CONFIG.batchMinGapMs);
  }
}

// Addresses and networks with a claim being processed right now. The instant
// path awaits a walletSend before the cooldown is persisted, so without this
// two simultaneous requests for the same address would both pass the cooldown
// check and both pay out. Reserved synchronously, with no await in between.
const inFlight = new Set();

// ---- HTTP -------------------------------------------------------------
function send(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    "content-type": "application/json",
    "access-control-allow-origin": CONFIG.allowedOrigin,
  });
  res.end(body);
}

function clientIp(req) {
  // Behind Caddy: X-Forwarded-For is set by the proxy, trust only that
  // header's first hop since this service is never meant to face the
  // internet directly.
  const xff = req.headers["x-forwarded-for"];
  return (xff ? xff.split(",")[0].trim() : req.socket.remoteAddress) || "unknown";
}

// floatMicroJtm is passed in rather than fetched here so this stays sync and
// so a claim reuses the balance it already read instead of asking twice.
// null means the node could not be reached, which the page shows as unknown
// rather than as zero.
function publicStatus(floatMicroJtm) {
  return {
    floatJtm: floatMicroJtm == null ? null : floatMicroJtm / 1_000_000,
    // One counter per mode: instant and batch cost the faucet different
    // totals, so a single number was only ever true for one of them.
    claimsLeftSingle: floatMicroJtm == null
      ? null
      : Math.floor(floatMicroJtm / (CONFIG.amountSingleMicroJtm + CONFIG.feeAllowanceMicroJtm)),
    claimsLeftBatch: floatMicroJtm == null
      ? null
      : Math.floor(floatMicroJtm / (CONFIG.amountBatchMicroJtm + CONFIG.feeAllowanceMicroJtm)),
    amountSingleJtm: CONFIG.amountSingleMicroJtm / 1_000_000,
    amountBatchJtm: CONFIG.amountBatchMicroJtm / 1_000_000,
    queueDepth: state.queue.length,
    // When the current queue goes out even if it never fills. null when the
    // queue is empty, because the clock starts with the first person to join.
    flushAt: state.nextFlushAt || null,
    heldUntil: heldUntil(),                      // full batch waiting out the gap
    batchMinGapMinutes: CONFIG.batchMinGapMs / 60000,
    batchMaxAgeHours: CONFIG.batchMaxAgeMs / 3600000,
    batchSize: CONFIG.batchSize,
    cooldownHours: CONFIG.cooldownHours,
    totalDispensedJtm: state.totalDispensedMicroJtm / 1_000_000,
    totalClaims: state.totalClaims,
    paused: state.paused,
    pausedReason: state.pausedReason,
    recentBatches: state.history.slice(0, 5).map((b) => ({
      batchId: b.batchId,
      completedAt: b.completedAt,
      sent: b.sends.filter((s) => s.ok).length,
      failed: b.sends.filter((s) => !s.ok).length,
    })),
  };
}

async function handleClaim(req, res) {
  let body = "";
  for await (const chunk of req) body += chunk;
  let parsed;
  try {
    parsed = JSON.parse(body || "{}");
  } catch {
    return send(res, 400, { ok: false, error: "malformed request" });
  }
  const address = String(parsed.address || "").trim();
  if (!validateAddress(address)) return send(res, 400, { ok: false, error: "not a valid j1… address" });
  if (state.paused) return send(res, 503, { ok: false, error: `faucet paused: ${state.pausedReason}` });

  // "single" pays out immediately and small; "batch" waits for the queue to
  // fill and pays more. Anything unrecognised falls back to batch.
  const single = parsed.mode === "single";
  const amount = single ? CONFIG.amountSingleMicroJtm : CONFIG.amountBatchMicroJtm;

  const ipHash = hashIp(clientIp(req));
  const now = Date.now();
  const cooldown = cooldownActiveMs();

  // ---- everything from here to the reservation must stay synchronous ----
  // ONE CLAIM PER MODE per window: a wallet may take the instant payout AND
  // ride a batch in the same 24h, but not two of either. Cooldown records are
  // therefore keyed `<mode>:<address>` (and `<mode>:<ipHash>`), so the two
  // modes hold independent windows and neither can be claimed twice.
  const modeKey = single ? "single" : "batch";
  const aKey = modeKey + ":" + address;        // cooldown key, this mode only
  const iKey = modeKey + ":" + ipHash;
  const flightA = "a:" + aKey;                 // in-flight reservation, ditto
  const flightI = "i:" + iKey;
  if (inFlight.has(flightA) || inFlight.has(flightI)) {
    return send(res, 429, { ok: false, error: "a claim for this address is already being processed" });
  }
  // Only blocks a SECOND batch claim; being queued does not bar the instant one.
  if (!single && state.queue.some((e) => e.address === address)) {
    return send(res, 429, { ok: false, error: "already queued for the next batch" });
  }
  const lastAddr = state.recentByAddress[aKey];
  if (lastAddr && now - lastAddr < cooldown) {
    const hrs = Math.ceil((cooldown - (now - lastAddr)) / 3600000);
    return send(res, 429, { ok: false,
      error: `already took the ${single ? "instant" : "batch"} claim — try again in ${hrs}h` });
  }
  const lastIp = state.recentByIp[iKey];
  if (lastIp && now - lastIp < cooldown) {
    return send(res, 429, { ok: false, error: "one claim per network per cooldown window" });
  }
  inFlight.add(flightA);
  inFlight.add(flightI);
  // ---- reservation held; awaits are safe from here ----

  try {
    const float = await cachedFloatMicroJtm();
    if (float === null) return send(res, 503, { ok: false, error: "faucet node unreachable, try again shortly" });
    // A single claim only needs to cover itself; a batch claim must not be
    // accepted unless the wallet can pay the whole batch it is joining.
    const needed = single ? amount + CONFIG.feeAllowanceMicroJtm : batchCostMicroJtm();
    if (float < needed) return send(res, 503, { ok: false, error: "faucet is out of funds for now" });

    if (single) {
      // The instant lane is capped by the protocol at one payment per block.
      // Refuse fast and say so, rather than making people wait or handing
      // them an opaque InsufficientFunds. Their claim is NOT consumed.
      // Taken here, synchronously, so a burst produces one winner and clean
      // refusals rather than a stampede of doomed sends.
      if (!tryAcquireSend()) {
        return send(res, 503, { ok: false, error: "another payment is going out right now — try again in a minute, or join the batch" });
      }
      let result;
      try {
        const spendable = (await rpcCall("walletGetBalance", [])).spendable_micro_jtm ?? 0;
        if (spendable < amount + CONFIG.feeAllowanceMicroJtm) {
          return send(res, 503, { ok: false, error: "the faucet can only send one payment per block — try again in a minute, or join the batch" });
        }
        // The cooldown is recorded only AFTER the send succeeds: a failed
        // send must not cost someone their daily claim for nothing.
        result = await rpcCall("walletSend", [address, amount, 0]);
      } catch (e) {
        console.error("single send failed for", address, e);
        return send(res, 502, { ok: false, error: "send failed, you have not used your claim — try again shortly" });
      } finally {
        sending = false;
      }
      state.recentByAddress[aKey] = now;
      state.recentByIp[iKey] = now;
      state.totalClaims += 1;
      state.totalDispensedMicroJtm += result.amount_micro_jtm;
      state.history.unshift({
        batchId: null,
        single: true,
        completedAt: Date.now(),
        sends: [{ address, txid: result.txid, amountMicroJtm: result.amount_micro_jtm, ok: true }],
      });
      state.history = state.history.slice(0, 50);
      floatCache = { value: null, at: 0 }; // balance just moved
      // Same figure the response returns, held as a floor so the next poll
      // cannot snap the displayed balance back to the stale confirmed one.
      const projected = Math.max(0, float - amount - CONFIG.feeAllowanceMicroJtm);
      notePendingSpend(projected);
      await saveState();
      return send(res, 200, {
        ok: true,
        mode: "single",
        txid: result.txid,
        amountJtm: result.amount_micro_jtm / 1_000_000,
        ...publicStatus(projected),
      });
    }

    state.queue.push({ address, ipHash, queuedAt: now, amountMicroJtm: amount });
    state.recentByAddress[aKey] = now;
    state.recentByIp[iKey] = now;
    state.totalClaims += 1;
    await saveState();

    send(res, 200, { ok: true, mode: "batch", position: state.queue.length, ...publicStatus(float) });
    maybeProcessBatch(); // fire-and-forget; response already sent
  } finally {
    inFlight.delete(flightA);
    inFlight.delete(flightI);
  }
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, "http://internal");
  res.setHeader("access-control-allow-origin", CONFIG.allowedOrigin);
  if (req.method === "OPTIONS") return send(res, 204, {});
  if (req.method === "GET" && url.pathname === "/api/status") return send(res, 200, publicStatus(await cachedFloatMicroJtm()));
  if (req.method === "GET" && url.pathname === "/api/health") return send(res, 200, { ok: true });
  if (req.method === "POST" && url.pathname === "/api/claim") return handleClaim(req, res);
  send(res, 404, { ok: false, error: "not found" });
});

// Age-out check. The interval scales with the deadline: a minute at the 24h
// default, proportionally tighter for short deadlines, so the behaviour is
// the same whether it is configured in hours or seconds.
const AGE_CHECK_MS = Math.max(500, Math.min(60_000, Math.floor(CONFIG.batchMaxAgeMs / 10)));
setInterval(() => {
  const dropped = pruneExpired();
  if (dropped) {
    console.log(`pruned ${dropped} expired cooldown record(s)`);
    saveState();
  }
  if (Date.now() < state.nextFlushAt) return;
  const n = state.queue.length;
  if (n > 0) {
    console.log(`cycle expired with ${n}/${CONFIG.batchSize} queued — sending it anyway`);
    maybeProcessBatch(true);
  } else {
    console.log("cycle expired with an empty queue — nothing to send");
  }
  // Roll forward whether or not anything went out, so the countdown on the
  // page is always live and always means the same thing.
  state.nextFlushAt = Date.now() + CONFIG.batchMaxAgeMs;
  saveState();
}, AGE_CHECK_MS);

await loadState();
if (!state.nextFlushAt
    || state.nextFlushAt < Date.now() - CONFIG.batchMaxAgeMs
    || state.nextFlushAt > Date.now() + CONFIG.batchMaxAgeMs) {
  // Reseed when the stored deadline is missing (pre-existing state file), long
  // stale (down past a whole window), or FURTHER OUT than the current window
  // allows -- the last case is someone shortening FAUCET_BATCH_MAX_AGE_HOURS,
  // where keeping the old deadline would silently ignore the new setting for
  // up to a full old cycle.
  state.nextFlushAt = Date.now() + CONFIG.batchMaxAgeMs;
  await saveState();
}
{
  const dropped = pruneExpired();
  if (dropped) {
    console.log(`pruned ${dropped} expired cooldown record(s) on startup`);
    await saveState();
  }
}
server.listen(CONFIG.port, CONFIG.bind, () => {
  console.log(`jetsam faucet backend on ${CONFIG.bind}:${CONFIG.port}`);
  console.log(`  single ${CONFIG.amountSingleMicroJtm / 1e6} JTM, batch ${CONFIG.amountBatchMicroJtm / 1e6} JTM x ${CONFIG.batchSize}, ${CONFIG.cooldownHours}h cooldown`);
  console.log(`  RPC target: ${CONFIG.rpcUrl}`);
});
