
{% macro generate_surrogate_key(field_list) %}

    md5(
        cast(
            concat_ws('|',
                {% for field in field_list %}
                    coalesce(cast({{ field }} as varchar), '_null_')
                    {%- if not loop.last %}, {% endif %}
                {% endfor %}
            ) as varchar
        )
    )

{% endmacro %}