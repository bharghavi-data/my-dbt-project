SELECT *
FROM {{ ref('test_table') }}
WHERE NOT USER_EMAIL RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
