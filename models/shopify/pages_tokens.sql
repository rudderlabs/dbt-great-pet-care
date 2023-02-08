{{ config( materialized='table') }}

select distinct  anonymous_id, context_session_id, user_id, split_part(context_page_path, '/', 4 ) as token
From {{source('shopify','pages')}} 
where len(split_part(context_page_path, '/', 4 )) = 32 -- specifically looking for tokens in the URL
