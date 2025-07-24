SELECT *
FROM {{ ref('test_table') }}
WHERE NOT USER_MOBILE RLIKE '^[0-9]{10}$'
