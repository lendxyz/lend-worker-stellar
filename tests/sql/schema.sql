-- Vendored test schema for lend_worker_stellar repository round-trip tests.
--
-- This is a TRIMMED, up-only copy of the subset of the lend-api migrations
-- that lend_worker_stellar's repositories actually touch (the `operations`,
-- `activity`, `dex_orders`, `fiat_holdings` tables, their enums, and the
-- `updated_at` trigger). It is intentionally self-contained so the worker's
-- tests need no access to the lend-api repo.
--
-- Source migrations (lend-api/migrations):
--   20250414094656_create_operations_table.sql
--   20250526121013_create_activity_and_users.sql
--   20250903152238_create_dex_orders.sql
--   20251017121858_add_updated_at_trigger.sql
--   20260427114311_fiat_rewards_holdings.sql
--
-- Keep in sync if those migrations change. Applied fresh on each test process.

DROP TABLE IF EXISTS activity CASCADE;
DROP TABLE IF EXISTS dex_orders CASCADE;
DROP TABLE IF EXISTS fiat_holdings CASCADE;
DROP TABLE IF EXISTS operations CASCADE;
DROP FUNCTION IF EXISTS updated_at_refresh() CASCADE;
DROP TYPE IF EXISTS activity_event_type CASCADE;
DROP TYPE IF EXISTS order_status CASCADE;
DROP TYPE IF EXISTS funding_status CASCADE;
DROP TYPE IF EXISTS operation_category CASCADE;
DROP TYPE IF EXISTS payout_interval CASCADE;

CREATE TYPE funding_status AS ENUM (
  'open', 'finished', 'predeposit', 'paused', 'upcoming', 'canceled'
);

CREATE TYPE payout_interval AS ENUM (
  'weekly', 'monthly', 'yearly', 'quarterly'
);

CREATE TYPE operation_category AS ENUM (
  'PROPERTY_REDEVELOPMENT', 'FLIP_OPERATION', 'LONG_TERM_RENTAL',
  'SHORT_TERM_RENTAL', 'BOND_FINANCING', 'NO_CATEGORY'
);

CREATE TYPE activity_event_type AS ENUM (
    'invested', 'refunded', 'invested_fiat', 'claimed_rewards',
    'claimed_ref_rewards', 'claimed_op_token', 'rewards_distributed',
    'ref_rewards_distributed', 'op_lend_bridged', 'op_lend_transfered',
    'op_lend_peer_added', 'op_paused', 'op_resumed', 'op_predeposits_open',
    'op_predeposits_closed', 'op_created', 'op_started', 'op_canceled',
    'op_finished', 'order_filled', 'order_cancelled'
);

CREATE TYPE order_status AS ENUM ('filled', 'open', 'cancelled');

CREATE TABLE operations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    category operation_category NOT NULL DEFAULT 'NO_CATEGORY',
    payout_interval payout_interval NOT NULL DEFAULT 'monthly',
    funding_status funding_status NOT NULL DEFAULT 'upcoming',
    funding_goal TEXT DEFAULT '0',
    shares_sold TEXT DEFAULT '0',
    stellar_shares_sold TEXT DEFAULT '0',
    funding_participants INT DEFAULT 0,
    total_shares TEXT DEFAULT '0',
    stellar_shares TEXT DEFAULT '0',
    supported_chains JSONB NOT NULL DEFAULT '[]',
    factory_op_id INT DEFAULT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE activity (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_hash TEXT UNIQUE NOT NULL DEFAULT NULL,
    chain_id INTEGER NOT NULL DEFAULT NULL,
    event_type activity_event_type NOT NULL,
    op_id UUID NOT NULL DEFAULT NULL,
    factory_op_id INTEGER NOT NULL DEFAULT NULL,
    user_address TEXT DEFAULT NULL,
    block_number INTEGER NOT NULL DEFAULT NULL,
    data JSONB NOT NULL DEFAULT '[]',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT fk_op FOREIGN KEY (op_id)
        REFERENCES operations(id) ON DELETE SET NULL
);

CREATE TABLE dex_orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
    factoryOpId INTEGER NOT NULL,
    salt TEXT UNIQUE NOT NULL,
    maker TEXT NOT NULL,
    receiver TEXT NOT NULL,
    makerAsset TEXT NOT NULL,
    takerAsset TEXT NOT NULL,
    makingAmount TEXT NOT NULL,
    takingAmount TEXT NOT NULL,
    makerTraits TEXT NOT NULL,
    orderHash TEXT NOT NULL,
    remainingMakingAmount TEXT NOT NULL,
    remainingTakingAmount TEXT NOT NULL,
    status order_status NOT NULL,
    r TEXT NOT NULL,
    vs TEXT NOT NULL,
    createdAt TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE fiat_holdings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
    user_address TEXT NOT NULL,
    value TEXT NOT NULL,
    factory_op_id INTEGER NOT NULL,
    withdrew_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE FUNCTION updated_at_refresh()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = (NOW() AT TIME ZONE 'UTC')::TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER updated_at_trigger_on_activity
BEFORE UPDATE ON activity
FOR EACH ROW EXECUTE FUNCTION updated_at_refresh();

CREATE TRIGGER updated_at_trigger_on_operations
BEFORE UPDATE ON operations
FOR EACH ROW EXECUTE FUNCTION updated_at_refresh();

CREATE TRIGGER updated_at_trigger_on_dex_orders
BEFORE UPDATE ON dex_orders
FOR EACH ROW EXECUTE FUNCTION updated_at_refresh();
