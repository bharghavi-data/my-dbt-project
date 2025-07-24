{% test user_id_regex(model, column_name) %}
SELECT * FROM {{ model }} WHERE NOT {{ column_name }} RLIKE '^[0-9]{1,3}$'
{% endtest %}