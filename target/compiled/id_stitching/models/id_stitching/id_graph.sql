

select
    rudder_id,
    edge,
     
    listagg(
        DISTINCT edge_label,
        ', '
        )
          as labels,
    max(edge_timestamp) as latest_timestamp
from (
    select
        rudder_id,
        edge_a as edge,
        edge_a_label as edge_label, 
        edge_timestamp
    from
        "dev"."rudderstack_dbt_dev"."edges"
    union
    select
        rudder_id,
        edge_b as edge,
        edge_b_label as edge_label, 
        edge_timestamp
    from
        "dev"."rudderstack_dbt_dev"."edges"
) c
group by
    rudder_id,
    edge
order by
    rudder_id