SELECT *
FROM {{ ref('test_table') }}
WHERE USER_EXP < 0 OR USER_EXP > 50
