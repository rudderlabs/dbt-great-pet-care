
  
    

  create  table
    "dev"."rudderstack_dbt_dev__6_a_907339_4_ae_5_406_f_ba_3_e_c_9_bf_93_f_03087"."pages_tokens__dbt_tmp"
    
    
    
  as (
    

select distinct  anonymous_id, context_session_id, user_id, split_part(context_page_path, '/', 4 ) as token
From "dev"."_6_a_907339_4_ae_5_406_f_ba_3_e_c_9_bf_93_f_03087"."pages" 
where len(split_part(context_page_path, '/', 4 )) = 32 -- specifically looking for tokens in the URL
  );
  