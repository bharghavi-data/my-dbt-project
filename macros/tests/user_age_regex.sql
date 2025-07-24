{% test user_age_regex(model, column_name) %}
SELECT * FROM {{ model }} WHERE NOT {{ column_name }} RLIKE '^[0-9]{1,3}$'
{% endtest %}