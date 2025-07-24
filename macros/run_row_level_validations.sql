{% macro validate_test_table_rules() %}

    {% set validations = [
        {
            'rule_id': 'R001',
            'column_name': 'USER_EMAIL',
            'rule_name': 'Email Format Check',
            'sql': "SELECT USER_ID, CASE WHEN USER_EMAIL ILIKE '%@%.%' THEN 'PASS' ELSE 'FAIL' END AS RESULT FROM DWH_HASTINGS_DATA_AI.TESTINGFRAMEWORK_AI.TEST_TABLE"
        },
        {
            'rule_id': 'R002',
            'column_name': 'USER_MOBILE',
            'rule_name': 'Mobile Number Format',
            'sql': "SELECT USER_ID, CASE WHEN LENGTH(USER_MOBILE::STRING) = 10 THEN 'PASS' ELSE 'FAIL' END AS RESULT FROM DWH_HASTINGS_DATA_AI.TESTINGFRAMEWORK_AI.TEST_TABLE"
        },
        {
            'rule_id': 'R003',
            'column_name': 'USER_EXP',
            'rule_name': 'Experience Range Check',
            'sql': "SELECT USER_ID, CASE WHEN USER_EXP BETWEEN 0 AND 50 THEN 'PASS' ELSE 'FAIL' END AS RESULT FROM DWH_HASTINGS_DATA_AI.TESTINGFRAMEWORK_AI.TEST_TABLE"
        }
    ] %}

    {% for rule in validations %}
        {% set rule_id = rule['rule_id'] %}
        {% set column_name = rule['column_name'] %}
        {% set rule_name = rule['rule_name'] %}
        {% set rule_sql = rule['sql'] %}

        {% set insert_sql %}
            INSERT INTO DWH_HASTINGS_DATA_AI.TESTINGFRAMEWORK_AI.TEST_LOG (
                USER_ID,
                COLUMN_NAME,
                VALIDATION_RULE_ID,
                VALIDATION_RULE_NAME,
                STATUS,
                REMARKS,
                EXECUTED_AT
            )
            {{ rule_sql | replace("SELECT", "SELECT") }}
            , '{{ column_name }}'
            , '{{ rule_id }}'
            , '{{ rule_name }}'
            , RESULT
            , '{{ rule_sql | replace("'", "''") }}'
            , CURRENT_TIMESTAMP()
        {% endset %}

        {% set final_sql %}
            INSERT INTO DWH_HASTINGS_DATA_AI.TESTINGFRAMEWORK_AI.TEST_LOG (
                USER_ID,
                COLUMN_NAME,
                VALIDATION_RULE_ID,
                VALIDATION_RULE_NAME,
                STATUS,
                REMARKS,
                EXECUTED_AT
            )
            SELECT
                USER_ID,
                '{{ column_name }}',
                '{{ rule_id }}',
                '{{ rule_name }}',
                RESULT,
                '{{ rule_sql | replace("'", "''") }}',
                CURRENT_TIMESTAMP()
            FROM (
                {{ rule_sql }}
            ) AS sub
        {% endset %}

        {% do run_query(final_sql) %}

    {% endfor %}

{% endmacro %}
