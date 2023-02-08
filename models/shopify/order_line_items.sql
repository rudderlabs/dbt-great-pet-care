{{ config(materialized='table', schema = '_6_a_907339_4_ae_5_406_f_ba_3_e_c_9_bf_93_f_03087') }}
	
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
	select token as order_token, id
	from {{source('shopify','order_created')}} c 
	where is_valid_json_array(products) = true	
	),

cte_valid_arrays_u as (
	select token as order_token, id 
	from {{source('shopify','order_updated')}} c 
	where is_valid_json_array(products) = true	
	)	
		

	Select distinct s.*, dense_rank() over (partition by order_token, product_id, id order by event_date desc ) as line_item_recency
	From 	(
		    SELECT cart_token as cart_token
				, token as order_token
				, number as order_number
				, checkout_id as checkout_id
				, checkout_token as checkout_token
		    	, id as event_id
				, event_text as event_text
		    	, convert_timezone('EDT', sent_at) as event_date
		    	, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'brand') AS brand
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'discounted_price') AS discounted_price
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'fulfillment_status') AS fulfillment_status
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
		    FROM {{source('shopify','order_created')}} , cte_seq seq 
		    WHERE seq.i < JSON_ARRAY_LENGTH(products)
				and id IN (Select distinct id from cte_valid_arrays_c) 

			UNION ALL  	

		    SELECT cart_token as cart_token
				, token as order_token
				, number as order_number
				, checkout_id as checkout_id
				, checkout_token as checkout_token
		    	, id as event_id
				, event_text as event_text
		    	, convert_timezone('EDT', sent_at) as event_date
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'brand') AS brand
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'discounted_price') AS discounted_price
				, json_extract_path_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(products, seq.i), 'fulfillment_status') AS fulfillment_status
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
		    FROM {{source('shopify','order_updated')}} , cte_seq seq 
		    WHERE seq.i < JSON_ARRAY_LENGTH(products)
				and id IN (Select distinct id from cte_valid_arrays_u) 
			) s 
