{{ config(materialized='table') }}


        Select feature_id, sql_to_run, null::boolean as sql_validated
        FROM 
                (
                Select feature_id
                        ,  'SELECT '
                ||	'rudder_id::text as rudder_id'					    -- unified user identifier
                ||	', '   ||  feature_id   || ' as feature_id'	            -- ID from the metrics definition table
                ||	', '   ||  'null'       || ' as time_window' 			-- Value of the segment (for example, the URL of the page call)
                ||	', '''   ||  segmentation || ''' as segment_id' 				-- Value of the segment (for example, the URL of the page call)
                ||	', '   ||  'max("timestamp")' || ' as last_time'	-- Max(timestamp) of the event or record being calculated
                ||	', ('  ||  coalesce(function, '')     || '(' || property || '))::text as metric_value'				-- the value calculated
                ||	', ''' ||  data_type    || ''' as data_type'					-- Data type of the metric
                ||	', ''' ||  GetDate()    || ''' as last_executed'				-- Date & Time this was last calculated
                ||	', '   ||  '''test'''   || ' as batch_id' 					-- Batch ID to identify the job/batch
                ||  ' FROM ' ||    event
                || Case when filter is not null then ' Where ' || filter else '' End 
                || Case when segmentation is not null then ' Group By ' || segmentation else '' End 
                as sql_to_run
                From {{source('metrics', 'rs_user_features_defs')}}
                ) s
        where sql_to_run is not null

