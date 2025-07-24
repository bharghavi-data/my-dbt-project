{% test user_id_duplicate_check(model, column_name) %}
SELECT {{ column_name }}, COUNT(*) FROM {{ model }} GROUP BY {{ column_name }} HAVING COUNT(*) > 1
{% endtest %}