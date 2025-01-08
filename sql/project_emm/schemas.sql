CREATE SCHEMA IF NOT EXISTS emm_schemas;

-- Create a new table named 'emm_project'
CREATE TABLE IF NOT EXISTS emm_project (
    id SERIAL PRIMARY KEY,                      -- Unique identifier for each row, automatically incremented
    name TEXT NOT NULL,                         -- Name of the project
    original_table_name TEXT NOT NULL,          -- The original table name for the project
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp column for creation time, defaults to current time
);

CREATE TABLE IF NOT EXISTS emm_permutation (
    id SERIAL PRIMARY KEY,                          -- Unique identifier for each row, automatically incremented
    name TEXT NOT NULL,                             -- Name of the project
    is_populated BOOLEAN,                           -- if the schema has been populated with data
    is_permutation BOOLEAN,                         -- if the schema represents a permutation
    permutation_id INTEGER,                         -- The permutation id if it is a permutation
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    -- Timestamp column for creation time, defaults to current time
    schema_id SERIAL references emm_project(id)     -- FK on schema
);

COMMENT ON TABLE emm_project IS 'Keep the project that were loaded in emm.';
COMMENT ON TABLE emm_permutation IS 'Keep all the schema being generated.';
