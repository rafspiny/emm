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

CREATE TABLE IF NOT EXISTS emm_analysis (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,                             -- For convenience, we keep the name as well
    description TEXT,                               -- Description of the analysis
    schema_id SERIAL references emm_project(id)     -- FK on schema
    type emm_analysis_type NOT NULL,                -- Type of analysis: SIZE, PERFORMANCE_RO, PERFORMANCE_RW
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    -- Timestamp column for creation time, defaults to current time
);

CREATE TABLE IF NOT EXISTS emm_analysis_report (
    id SERIAL PRIMARY KEY,
    analysis_id SERIAL references emm_analysis(id) NOT NULL  -- FK on schema
    metric TEXT NOT NULL,                                    -- For convenience, we keep the name as well
    best_permutation_name TEXT NOT NULL,                     -- The name of the best permutation
    improvement_percentage_over_baseline DOUBLE NOT NULL,    -- Improvement percentage over the baseline
    size_table INT NOT NULL,                                 -- Size of the table, for validation purposes
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,             -- Timestamp column for creation time, defaults to current time
);

CREATE TABLE IF NOT EXISTS emm_raw_performance (
    id SERIAL PRIMARY KEY,
    analysis_id SERIAL references emm_analysis(id) NOT NULL         -- FK on schema
    permutation_id SERIAL references emm_permutation(id) NOT NULL   -- FK on schema
--    workload_id SERIAL references emm_workload(id) NULL           -- FK on schema
    metric TEXT,                                                    -- Metric name (row_estimate, index_bytes, toast_bytes, table_bytes, total_bytes, metric_value)
    notes TEXT,                                                     -- For convenience, we keep the name as well
    value BIGINT,                                                   -- The value of the metric
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,                    -- Timestamp column for creation time, defaults to current time
);


COMMENT ON TABLE emm_project IS 'Keep the project that were loaded in emm.';
COMMENT ON TABLE emm_permutation IS 'Keep all the schema being generated.';
COMMENT ON TABLE emm_analysis IS 'Hold type of analysis ran on schemas.';
COMMENT ON TABLE emm_analysis_report IS 'Keep the report for each analysis.';
COMMENT ON TABLE emm_raw_performance IS 'Keep the raw data for the analysis, per permutation based.';
