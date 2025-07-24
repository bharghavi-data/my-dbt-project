{% test user_country_regex(model, column_name) %}
SELECT * FROM {{ model }} WHERE NOT {{ column_name }} RLIKE '^[A-Za-z ]+$'
{% endtest %}