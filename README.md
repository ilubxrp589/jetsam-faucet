# Jetsam Faucet

A small, deliberately boring faucet for [Jetsam](https://jetsamchain.com) (JTM).
Two claim lanes, one claim per lane per wallet per day, batched payouts on a
fixed departure schedule.

**Live:** https://jtmfaucet.halcyon-names.io
*Community project — not an official Jetsam project.*

---

## How it works

Two lanes, and a wallet may use **each once per 24h**:

| Lane | Pays | When |
|---|---|---|
| **Instant** | less | immediately, returns a txid |
| **Batch** | more | at the next scheduled departure |

A batch **departs on a schedule** (default every 6h, so four a day) carrying
whoever is aboard. If nobody queued, nothing is sent and the clock rolls over.
A batch that fills early still waits for its slot — see *One send per block*
below for why that matters.

The page shows a live countdown to the next departure, and says plainly whether
anyone is actually aboard, so a running clock never implies a payout that is
not coming.

## Security model

- **Separate hot wallet.** The faucet has its own node and its own wallet, funded
  from elsewhere. It is internet-triggered, so it holds only what it can afford
  to lose. Wallet RPC is bound to loopback and never exposed.
- **One claim per mode, per wallet *and* per network**, per window. Cooldown
  records are keyed `<mode>:<address>` and `<mode>:<ip-hash>`.
- **IPs are hashed**, never stored raw.
- **Reservation before any `await`.** The claim path reserves address+IP
  *synchronously* before the first await. Without that, simultaneous requests
  for one address all pass the cooldown check and all get paid. Verified: five
  concurrent claims → exactly one payment.
- **Cooldown is written only after a send succeeds**, so a failed send never
  costs someone their daily claim.
- **Auto-pause** when the float can no longer cover a batch.
- Runtime state (`faucet-data.json`) holds claimant addresses and hashed IPs and
  is **gitignored**.

## Building on Jetsam: things that cost us time

Most of these are not in any document, and each one presents as something else.

1. **RPC methods are namespaced.** The wire name is `jetsam_walletSend`, not
   `walletSend`. A bare name returns `-32601` and reads exactly like a dead node.
2. **One send per block.** The wallet's input is locked until its change
   confirms, so `spendable` drops to **zero for a whole block** after every send
   and the next one fails with InsufficientFunds. Sends must be serialised —
   a batch of N takes N blocks. This is the real throughput ceiling, not the float.
3. **Transactions cap at 2 outputs.** There is no multi-recipient transaction, so
   a batch of 10 is 10 separate sends, each paying its own fee.
4. **The fee is a formula, not a market — and a payout is charged for state
   growth.** There is no fee bidding. From
   `jetsam_chain/src/consensus/{params,fees}.rs`:

   ```
   required = MIN_FEE_BASE                       (5,000 uJTM)
            + FEE_PER_INPUT   x inputs           (  100 each)
            + FEE_PER_OUTPUT  x outputs          (  700 each)
            + state_growth    x (outputs - inputs)

   state_growth = STATE_GROWTH_FEE_BASE (2,500) x pressure_multiplier
   pressure_multiplier: 1, then x2 / x4 / x8 at 50% / 75% / 90% slot occupancy
   ```

   Only `state_growth` is **burned**; the rest is claimable by the miner.

   A faucet payout is 1 input and 2 outputs (recipient + change), so it is
   **net +1 slot** and costs `5,000 + 100 + 1,400 + 2,500 = 9,000 uJTM`. That
   is ~30% of a 0.03 payout and ~90% of a 0.01 one — the single biggest cost
   in running a faucet.

   You will see cheaper transactions on chain (6,600 is common). Those are
   **net-zero-slot** — e.g. 2-in/2-out consolidations and transfers, which pay
   no state-growth component at all. That saving is not available to a faucet:
   handing someone a new UTXO *is* state growth, and the protocol prices it
   deliberately. Restructuring does not help either — a 1-in/1-out send with no
   change costs 5,800, but creating each exact-denomination UTXO to enable it
   is itself net +1 slot at 9,000. The arithmetic is negative in both
   directions.

   **So pass `fee = 0` (automatic).** The node computes the required fee plus
   the live mempool floor. Hardcoding a lower number risks a transaction that
   never relays, which costs a claimer their cooldown for nothing.

   The pre-flight check must reserve a fee *per send*, or the faucet accepts a
   full batch it can only partly pay. Reserve above the current figure: the
   pressure multiplier is stepwise, so the fee jumps to 11,500 the moment slot
   occupancy crosses 50%.
5. **`balance_micro_jtm` is confirmed-only.** It does not reflect a pending
   outbound send, so right after paying someone the node still reports the old
   figure until the change confirms. A UI that polls it will show the balance
   snap back. This faucet holds the projected value as a floor until the node
   catches up. `spendable` is not a substitute — it reads 0 for a whole block.
6. **Node version matters for sync.** An older binary may connect to peers and
   then reject every snapshot with `unsupported HistoryStep version N`. Run a
   current node.
7. **Share links:** use `x.com/intent/tweet`, not `/intent/post`. The app's
   universal-link handler has no route for the latter and bounces back to the
   browser, causing a refresh loop on mobile.

## Running it

Requires Node 20+ and a Jetsam node with a funded wallet.

```bash
git clone https://github.com/ilubxrp589/jetsam-faucet
cd jetsam-faucet
cp deploy/jetsam-faucet-api.service.example /tmp/jetsam-faucet-api.service
# edit: User, paths, FAUCET_ALLOWED_ORIGIN, amounts, schedule
sudo cp /tmp/jetsam-faucet-api.service /etc/systemd/system/
sudo systemctl enable --now jetsam-faucet-api
```

Serve `public/` with any static web server and reverse-proxy `/api/*` to the
backend. Every tunable is an `Environment=` line in the unit, and the page reads
amounts, schedule and counters from `/api/status` — so changing the unit changes
the UI.

## Layout

```
server.mjs                    the whole backend, no dependencies
public/index.html             the whole frontend, no build step
public/art/                   harbour scene, 4 times of day x 2 aspects x 2 DPR
deploy/                       example systemd unit
```

No framework, no build, no database. One file each side.

## License

Apache-2.0, matching Jetsam itself.
