
      
        
            delete from "dev"."rudderstack_dbt_dev"."edges"
            where (
                original_rudder_id) in (
                select (original_rudder_id)
                from "edges__dbt_tmp201601641850"
            );

        
    

    insert into "dev"."rudderstack_dbt_dev"."edges" ("rudder_id", "original_rudder_id", "edge_a", "edge_a_label", "edge_b", "edge_b_label", "edge_timestamp")
    (
        select "rudder_id", "original_rudder_id", "edge_a", "edge_a_label", "edge_b", "edge_b_label", "edge_timestamp"
        from "edges__dbt_tmp201601641850"
    )
  