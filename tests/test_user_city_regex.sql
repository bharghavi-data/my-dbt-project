SELECT *
FROM {{ ref('test_table') }}
WHERE NOT USER_CITY RLIKE '^[A-Za-z ]+$'
