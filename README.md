# Insurance DV 2.0 + Medallion (PySpark + Delta)

**Goal:** show a clean Raw Vault on Delta, plus a minimal Business Vault + Gold so analysts don’t need to learn DV.

## Architecture
Bronze (raw files) → Silver (Raw Vault: Hubs/Links/SATs) → BV (small snapshots/bridges) → Gold (dims/facts).

## Data Model (grains)
- Hubs: `hub_customer_daily (customer_id)`, `hub_policy_daily (policy_number)`, `hub_agent_daily (license_number)`
- Links: `link_customer_policy (customer_id, policy_number)`, `link_policy_agent (policy_number, license_number)`
- SATs (SCD2): `sat_customer_personinfo`, `sat_policy_details`, `sat_agent_details`
- Events: `sat_policy_status_event` (append)
- BV: `policy_current_status`, `customer_360`
- Gold views: `gd.dim_customer_current`, `gd.dim_agent_current`, `gd.fact_policy_current`

## Quickstart
1. `src_staging/` → load Bronze sample
2. `silver_raw_vault/` → build hubs, links, sats (MERGE with SCD2)
3. `silver_business_vault/` → upsert `policy_current_status`
4. `gold_domain/customer/` → build `customer_360`
