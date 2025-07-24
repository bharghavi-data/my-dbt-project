{{ config(
    materialized='incremental',
    unique_key=['load_id', 'unique_id'],
    incremental_strategy='insert_overwrite',
    partition_by={"field": "load_id", "data_type": "timestamp"}
) }}

SELECT
    load_id,
    unique_id,
    status,
    TRY_CAST(failures AS INTEGER) AS failures,
    message,
    compiled_code,
    timestamp AS test_timestamp,        
    CURRENT_TIMESTAMP() AS load_timestamp
FROM {{ ref('dq_test_results_staging') }}
