SELECT USER_ID, COUNT(*) AS count
FROM {{ ref('test_table') }}
GROUP BY USER_ID
HAVING COUNT(*) > 1
