{{ config(materialized='table') }}

{% set features_to_pivot = run_query('select feature_id, feature_name, data_type from rudderstack.rs_user_features_defs where feature_id is not null ')%}

with 

rs_features as (

   select * from {{ ref('rs_features') }}

),
 
pivot_features_by_rudder_id as (
   
   select
      rudder_id     
      {% for feature_id in features_to_pivot -%}
 
         , max(
            case
               when feature_id = {{feature_id[0]}} 
               then metric_value::{{feature_id[2]}}
               else null 
            end
         ) as  {{ feature_id[1] }}


      {%- endfor %}
      , GetDate() as date_compiled

   from rs_features

   group by 1

)
 
select * from pivot_features_by_rudder_id