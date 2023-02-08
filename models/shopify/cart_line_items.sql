{{ config(materialized='table') }}
	
with  
cte_seq as  (
	{%- for i in range(0, 20) %}
      select {{ i }} as i
      {%- if not loop.last %}
          union all 
      {% endif %}
  	{%- endfor %}
	),

cte_valid_arrays_c as (
	select token as cart_token, id
	from {{source('shopify','cart_create')}} c 
	where is_valid_json_array(products) = true	
	),

cte_valid_arrays_u as (
	select token as cart_token, id 
	from {{source('shopify','cart_update')}} c 
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
		    FROM {{source('shopify','cart_create')}} , cte_seq seq 
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
		    FROM {{source('shopify','cart_update')}} , cte_seq seq 
		    WHERE seq.i < JSON_ARRAY_LENGTH(products)
				and id IN (Select distinct id from cte_valid_arrays_u) 
			) s 
