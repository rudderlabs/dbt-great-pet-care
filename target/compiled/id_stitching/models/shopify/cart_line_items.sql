
	
with  
cte_seq as  (
      select 0 as i
          union all 
      
      select 1 as i
          union all 
      
      select 2 as i
          union all 
      
      select 3 as i
          union all 
      
      select 4 as i
          union all 
      
      select 5 as i
          union all 
      
      select 6 as i
          union all 
      
      select 7 as i
          union all 
      
      select 8 as i
          union all 
      
      select 9 as i
          union all 
      
      select 10 as i
          union all 
      
      select 11 as i
          union all 
      
      select 12 as i
          union all 
      
      select 13 as i
          union all 
      
      select 14 as i
          union all 
      
      select 15 as i
          union all 
      
      select 16 as i
          union all 
      
      select 17 as i
          union all 
      
      select 18 as i
          union all 
      
      select 19 as i
	),

cte_valid_arrays_c as (
	select token as cart_token, id
	from "dev"."_6_a_907339_4_ae_5_406_f_ba_3_e_c_9_bf_93_f_03087"."cart_create" c 
	where is_valid_json_array(products) = true	
	),

cte_valid_arrays_u as (
	select token as cart_token, id 
	from "dev"."_6_a_907339_4_ae_5_406_f_ba_3_e_c_9_bf_93_f_03087"."cart_update" c 
	where is_valid_json_array(products) = true	
	)	
		

	Select distinct s.*, dense_rank() over (partition by cart_token, product_id, id order by event_date desc ) as line_item_recency
	From 	(
		    SELECT token as cart_token
		    	, id as event_id
		    	, convert_timezone('EDT', sent_at) as event_date
		    	, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'brand') AS brand
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'discounted_price') AS discounted_price
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'gift_card') AS gift_card
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'grams') AS grams
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'id') AS id
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'key') AS key
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'line_price') AS line_price
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'original_line_price') AS original_line_price
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'original_price') AS original_price
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'price') AS price
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'product_id') AS product_id
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'properties') AS properties
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'quantity') AS quantity
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'sku') AS sku
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'taxable') AS taxable
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'title') AS title
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'total_discount') AS total_discount
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'variant') AS variant
		    FROM "dev"."_6_a_907339_4_ae_5_406_f_ba_3_e_c_9_bf_93_f_03087"."cart_create" , cte_seq seq 
		    WHERE seq.i < JSON_ARRAY_LENGTH(products)
				and id IN (Select distinct id from cte_valid_arrays_c) 

			UNION   	

		    SELECT token as cart_token
		    	, id as event_id
		    	, convert_timezone('EDT', sent_at) as event_date
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'brand') AS brand
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'discounted_price') AS discounted_price
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'gift_card') AS gift_card
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'grams') AS grams
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'id') AS id
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'key') AS key
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'line_price') AS line_price
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'original_line_price') AS original_line_price
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'original_price') AS original_price
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'price') AS price
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'product_id') AS product_id
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'properties') AS properties
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'quantity') AS quantity
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'sku') AS sku
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'taxable') AS taxable
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'title') AS title
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'total_discount') AS total_discount
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'variant') AS variant
		    FROM "dev"."_6_a_907339_4_ae_5_406_f_ba_3_e_c_9_bf_93_f_03087"."cart_update" , cte_seq seq 
		    WHERE seq.i < JSON_ARRAY_LENGTH(products)
				and id IN (Select distinct id from cte_valid_arrays_u) 
			) s