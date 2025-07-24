SELECT *
FROM {{ ref('test_table') }}
WHERE NOT USER_ADDRESS RLIKE '^[A-Za-z0-9 ,.-]+$'

