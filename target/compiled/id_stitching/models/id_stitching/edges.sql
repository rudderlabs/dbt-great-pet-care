





      with 
        cte_min_edge_1 as (
            select edge, min(rudder_id) as first_row_id
                From 
                (
                    Select rudder_id, lower(edge_a) as edge
                    From "dev"."rudderstack_dbt_dev"."edges"

                    UNION

                    Select rudder_id, lower(edge_b) as edge
                    From "dev"."rudderstack_dbt_dev"."edges"
                ) c
            Group by edge
            ),

        cte_min_edge_2 as (
            select edge, min(rudder_id) as first_row_id
                From 
                (
                    select least(a.first_row_id,  b.first_row_id) as rudder_id,
                        lower(o.edge_a) as edge
                    from "dev"."rudderstack_dbt_dev"."edges" o
                      left outer join cte_min_edge_1 a on lower(o.edge_a) = a.edge -- already lowercased in prior step
                      left outer join cte_min_edge_1 b on lower(o.edge_b) = b.edge -- already lowercased in prior step
                
                    UNION

                    select least(a.first_row_id,  b.first_row_id) as rudder_id,
                        lower(o.edge_b) as edge
                    from "dev"."rudderstack_dbt_dev"."edges" o
                      left outer join cte_min_edge_1 a on lower(o.edge_a) = a.edge 
                      left outer join cte_min_edge_1 b on lower(o.edge_b) = b.edge 
                  
                )
            Group by edge
        ),

        cte_min_edge_3 as (
            select edge, min(rudder_id) as first_row_id
                From 
                (
                    select least(a.first_row_id,  b.first_row_id) as rudder_id,
                        lower(o.edge_a) as edge
                    from "dev"."rudderstack_dbt_dev"."edges" o
                      left outer join cte_min_edge_2 a on lower(o.edge_a) = a.edge 
                      left outer join cte_min_edge_2 b on lower(o.edge_b) = b.edge 
                    
                    UNION

                    select least(a.first_row_id,  b.first_row_id) as rudder_id,
                        lower(o.edge_b) as edge
                    from "dev"."rudderstack_dbt_dev"."edges" o
                      left outer join cte_min_edge_2 a on lower(o.edge_a) = a.edge 
                      left outer join cte_min_edge_2 b on lower(o.edge_b) = b.edge 
                    
                )
            Group by edge
        ),

        cte_new_id as (
            select
                least(a.first_row_id, b.first_row_id) as new_rudder_id,
                o.original_rudder_id
            From "dev"."rudderstack_dbt_dev"."edges" o 
              left outer join cte_min_edge_3 a on lower(o.edge_a) = a.edge 
              left outer join cte_min_edge_3 b on lower(o.edge_b) = b.edge
        
          ) 


        Select  n.new_rudder_id as rudder_id,
            e.original_rudder_id,
            e.edge_a,
            e.edge_a_label,
            e.edge_b,
            e.edge_b_label,
            
    getdate()
 as edge_timestamp
        From "dev"."rudderstack_dbt_dev"."edges" e
            Inner Join cte_new_id n ON  e.original_rudder_id = n.original_rudder_id 
        where e.rudder_id <> n.new_rudder_id        

