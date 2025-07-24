SELECT *
FROM {{ ref('test_table') }}
WHERE NOT USER_ID RLIKE '^[0-9]{1,3}$'
