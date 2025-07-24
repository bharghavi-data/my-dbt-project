{{ config(
    materialized='view'
) }}

WITH base AS (
    SELECT *,
           DENSE_RANK() OVER (ORDER BY timestamp) AS load_id  -- simulate numeric load_id
    FROM {{ ref('test_log_output') }}
)

SELECT * FROM base
