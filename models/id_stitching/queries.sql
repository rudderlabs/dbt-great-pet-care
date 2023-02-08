with columns as (
    select
        '"' || table_catalog || '"."' || table_schema || '"."' || table_name  || '"' as tn,
        column_name as cn
    from SVV_COLUMNS
    where
        lower(column_name) in {{ var('id-columns') }}
        AND UPPER(TABLE_NAME) IN ('PAGES', 'PAGES_TOKENS', 'IDENTIFIES', 'TRACKS', 'CHECKOUT_UPDATED', 'ORDER_CREATED', 'ORDER_UPDATED', 'CHECKOUT_STARTED' , 'CARTS_CREATE', 'CARTS_UPDATE' )
)
select
    'select distinct (' || a.cn || '::text) as edge_a
        , (''' || a.tn  || '' || '.' || a.cn  || ''') as edge_a_label
        , (' || b.cn || '::text) as edge_b
        , (''' || b.tn || '.' || b.cn  || ''') as edge_b_label 
        from ' ||  a.tn || ' where coalesce(' || a.cn ||  '::text, '''') <> '''' and coalesce(' || b.cn ||  '::text, '''') <> ''''' as sql_to_run
from
    columns a
inner join
    columns b 
        on a.tn = b.tn
        and a.cn > b.cn
        