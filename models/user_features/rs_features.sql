{{ config(materialized='table')}}


{% set features_to_build = run_query('Select feature_id, sql_to_run from rudderstack.rs_feature_defs where feature_id is not null order by 1')%}

Select * FROM (

        {% for feature_id in features_to_build -%}
                
        {{ feature_id[1] }} 

                {% if not loop.last %}
        
                UNION ALL 

                {% endif %}  
        
        {% endfor %}

) insert_statement
