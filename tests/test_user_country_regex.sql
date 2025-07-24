SELECT *
FROM {{ ref('test_table') }}
WHERE NOT USER_COUNTRY RLIKE '^[A-Za-z ]+$'
