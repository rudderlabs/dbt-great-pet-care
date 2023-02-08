with cte_user_journey as (
  Select distinct * 
  From 
      (
          Select distinct
              'Page' as event
              , p.title as event_text
              , p.anonymous_id
              , p.id as event_id
              , coalesce(p.user_id, (case when coalesce(u1.labels, u2.labels) = 'user_id' then coalesce(u1.edge, u2.edge) else null end) ) as user_id
              , case when coalesce(u1.labels, u2.labels) = 'email' then coalesce(u1.edge, u2.edge) else null end  as email
              , p.timestamp
              , p.channel
              --, p.context_campaign_click as campaign_click
              --, p.context_campaign_content as campaign_content
              , p.context_campaign_medium as campaign_medium
              , p.context_campaign_name as campaign_name
              , p.context_campaign_source as campaign_source
              --, p.context_campaign_term as campaign_term
              , p.context_page_referrer as referrer
              , p.context_page_url as url
              , p.title 
              , p.context_request_ip as ip
              , case when p.user_id is null and coalesce(u1.edge, u2.edge) is not null 
                     then 'Unknown' else 'Known' 
                     end as event_timing
        	  , coalesce(u1.rudder_id::text, u2.rudder_id::text, p.anonymous_id) as rudder_id
          From {{ source('rudderstack', 'pages') }} p 
             Left Outer Join {{ ref('id_graph') }} u1 on p.anonymous_id = u1.edge 
             Left Outer Join {{ ref('id_graph') }} u2 on p.user_id = u2.edge 

        	union all 

          Select Distinct
              t.event
              , t.event_text
              , t.anonymous_id
              , t.id as event_id
              , coalesce(t.user_id, (case when coalesce(u1.labels, u2.labels) = 'user_id' then coalesce(u1.edge, u2.edge) else null end) ) as user_id
              , case when coalesce(u1.labels, u2.labels) = 'email' then coalesce(u1.edge, u2.edge) else null end  as email
              , t.timestamp
              , t.channel
              --, t.context_campaign_click as campaign_click
              --, t.context_campaign_content as campaign_content
              , t.context_campaign_medium as campaign_medium
              , t.context_campaign_name as campaign_name
              , t.context_campaign_source as campaign_source
              --, t.context_campaign_term as campaign_term
              , t.context_page_referrer as referrer
              , t.context_page_url as url
              , t.context_page_title as title
              , t.context_request_ip as ip
              , case when t.user_id is null and coalesce(u1.edge, u2.edge) is not null 
                     then 'Unknown' else 'Known' 
                     end as event_timing
              , coalesce(u1.rudder_id::text, u2.rudder_id::text, t.anonymous_id) as rudder_id
          From {{ source('rudderstack', 'tracks') }} t 
              Left Outer Join {{ ref('id_graph') }} u1 on t.anonymous_id = u1.edge 
              Left Outer Join {{ ref('id_graph') }} u2 on t.user_id = u2.edge 

          UNION ALL 

          Select Distinct
              'Identify' as event
              , 'Identify' as event_text
              , t.anonymous_id
              , t.id as event_id
              , t.user_id as user_id
              , case when u1.labels like '%email%' then u1.edge else null end  as email
              , t.timestamp
              , t.channel
              --, t.context_campaign_click as campaign_click
              --, t.context_campaign_content as campaign_content
              , t.context_campaign_medium as campaign_medium
              , t.context_campaign_name as campaign_name
              , t.context_campaign_source as campaign_source
              --, t.context_campaign_term as campaign_term
              , t.context_page_referrer as referrer
              , t.context_page_url as url
              , t.context_page_title as title
              , t.context_request_ip as ip
              , 'Known' as event_timing -- Always known for identify calls
              , coalesce(u1.rudder_id::text,  t.user_id) as rudder_id
          From {{ source('rudderstack', 'identifies') }} t 
              Left Outer Join {{ ref('id_graph') }} u1 on t.user_id = u1.edge 
      ) user_journey
)

Select * 
    , extract(seconds from (timestamp - lag(timestamp) 
         over (order by rudder_id, timestamp)))  as time_lag
    , dense_rank() over (partition by rudder_id 
          order by timestamp asc, case when event = 'Page' then 1 else 2 end) 
          as user_event_step
from cte_user_journey
order by timestamp 


