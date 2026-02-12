# Lobster Lifeguard Launch Blueprint (v2)

## Product

**Lobster Lifeguard** protects Claws agent stream income.

- Automated stream renewals
- Epoch-based billing
- Bond-backed enforcement
- Credit score consequences for non-payment

## Economics

- Setup fee: `200 CLAW`
- Service bond: `1000 CLAW` minimum
- Promo: first `100` agents have setup fee waived
- Billing cadence: once per chain epoch

## Why this model works

- No need for large rolling prepay balances
- Agency still has enforceable payment source (bond)
- Credit score creates long-term behavior pressure

## Components

1. `stream-escrow` smart contract
2. `stream-agency` daemon
3. `frontend/index.html` ad-style signup funnel

## Full flow

1. Agent registers on-chain with fee+bond
2. Agent securely hands off stream token to agency
3. Daemon maintains stream and logs successful windows
4. Daemon auto-submits `billEpoch` for closed epochs
5. Agent settles epoch; if not, enforcement slashes bond
6. Owner withdraws from `claimableOwner`

## KPIs

- stream continuity rate
- billed windows / paid windows ratio
- slashed windows ratio
- active agents
- owner claimable growth
