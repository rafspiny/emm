CREATE SCHEMA IF NOT EXISTS emm_schemas;

-- Create a new table named 'emm_schema'
CREATE TABLE IF NOT EXISTS emm_schema (
    id SERIAL PRIMARY KEY,        -- Unique identifier for each row, automatically incremented
    name TEXT NOT NULL,           -- Name column with
    is_populated BOOLEAN,         -- Boolean column for 'is_populated'
    is_permutation BOOLEAN,       -- Boolean column for 'is_permutation'
    permutation_id INTEGER,       -- Integer column for 'permutation_id'
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp column for creation time, defaults to current time
);

COMMENT ON TABLE emm_schema IS 'Keep the schemas created for analys.';