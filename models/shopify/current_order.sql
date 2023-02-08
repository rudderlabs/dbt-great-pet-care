{{ config(materialized='table') }}

{%set table_query %}
	select distinct table_schema || '.' || table_name as table_name, table_name as t_name
	from svv_columns c 
	where table_name IN {{ var('shopify_order_tables') }} 
	
{% endset %}

{% set all_tables = run_query(table_query) %}

--- Outer Table -- Grab the most recent record 

Select dense_rank() over (partition by order_number order by timestamp desc ) as current_order_row, *
From (

{% for t in all_tables %} -- find all the possible columns and create placeholders if the don't exist

		{% set col_query %}
				with cte_col_list as ( -- Find all of the column names 
					select distinct column_name 
					from svv_columns c 
					where table_name IN {{ var('shopify_order_tables') }}
				),

				 cte_tbl_list as ( -- Create the list of all possible columns and tables
						select distinct table_name , c.column_name
						from svv_columns t,  cte_col_list c
						where table_name IN {{ var('shopify_order_tables') }}
					)

				-- Determine whether we need to pass a NULL as a placeholder for each column				
				Select Distinct t.table_name, t.column_name, IsNull(c.column_name, 'NULL') || ' as ' || t.column_name as c_sql
				from cte_tbl_list t 
						left outer join svv_columns c on t.table_name = c.table_name and t.column_name = c.column_name
				where t.table_name = ('{{t[1]}}')
				order by t.column_name	

		{% endset %}

		{% set all_cols = run_query(col_query) %}

Select 	{% for col in all_cols -%}
  		{{ col[2] }} {% if not loop.last %} , {% endif %}  
        {% endfor %}
From {{ t[0] }}

{% if not loop.last %} 

UNION ALL 

{% endif %}  
{% endfor %}

-- End 
) as z 
order by order_number, 1