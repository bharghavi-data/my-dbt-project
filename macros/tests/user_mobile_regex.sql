{% test user_mobile_regex(model, column_name) %}
SELECT * FROM {{ model }} WHERE NOT {{ column_name }} RLIKE '^[0-9]{10}$'
{% endtest %}