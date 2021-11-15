{% macro sum_sales_by_category(product_categories) %}
{%- for product in product_categories -%}
sum(case when product_category = '{{ product }}' 
    then product_price_at_time_of_order end) 
    as {{ product | replace(' ', '_')  }}_amount,
{% endfor %}
{% endmacro %}