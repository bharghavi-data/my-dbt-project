{% test user_address_regex(model, column_name) %}
SELECT * FROM {{ model }} WHERE NOT {{ column_name }} RLIKE '^[A-Za-z0-9 ,.-]+$'
{% endtest %}