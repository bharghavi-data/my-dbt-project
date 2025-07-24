SELECT *
FROM {{ ref('test_table') }}
WHERE NOT USER_AGE RLIKE '^[0-9]{1,3}$'
