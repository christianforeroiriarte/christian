# Databricks notebook source
# MAGIC %pip install pymssql==2.2.7

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

# COMMAND ----------

import pandas as pd 
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import datetime

def execute_query(query):
    import pymssql
    server = 'gripsqlserver.database.windows.net'
    db = 'gripsqldb'
    un = 'grip_owner'
    pw = dbutils.secrets.get('gripkv', 'grip-owner-login-pw')   
    conn = pymssql.connect(server, un, pw, db)
    cursor = conn.cursor(as_dict=True)
    cursor.execute(query)
    results = []
    for row in cursor:
        results.append(row)
    conn.commit()
    conn.close()
    return results
  
def get_query_for_batch(table_name, id_filter_value=0, id_filter_name='id', batch_size=10000, days_back=4):
    query = f'''
    SELECT 
        TOP {batch_size} * 
    FROM {table_name} 
        WHERE modified_datetime >= dateadd(day, -{days_back}, cast(getdate() as date)) AND {id_filter_name} > {id_filter_value} 
    ORDER BY cast({id_filter_name} as int) ASC 
    '''

    return query

def get_query_batch_max_id(df, id_filter_name='id'):
    max_id = df.selectExpr(f'max(cast({id_filter_name} as integer))').collect()[0][0]
    return max_id
 

# COMMAND ----------

# DBTITLE 1,Get Tables To Load
# get tables to load 
tables_to_load = []
tables = execute_query(f"SELECT * FROM sys.tables WHERE schema_id = 5")
for t in tables:
    tables_to_load.append({
        'table_name': f"beta.{t['name']}",
        'output_folder': t['name'],
        'output_database':'grip_platform'
    })
    
# # if hardcoding
# tables_to_load = [
#     {'table_name': 'beta.carrier_ziptable', 'output_folder': 'carrier_ziptable', 'output_database': 'grip_platform'}
# #     {'table_name': 'beta.warehouses', 'output_folder': 'warehouses', 'output_database': 'grip_platform'},
# #     {'table_name': 'beta.orders', 'output_folder': 'orders', 'output_database': 'grip_platform'},
# #     {'table_name': 'beta.customers', 'output_folder': 'customers', 'output_database': 'grip_platform'} 
# ]

# tables_to_load = [{'table_name': 'beta.carrier_ziptable', 'output_folder': 'carrier_ziptable', 'output_database': 'grip_platform'}]

# COMMAND ----------

# DBTITLE 1,Define If Any Tables To Skip & Drop Old Ones If Needed
skip = [
    'beta.weather_forecasts',
    'beta.ziptable',
    'beta.tokens',
    'beta.tokens_permissions',
    'beta.users_customers',
    'beta.users_tokens',
#     'beta.orders_v2'
#     'beta.carrier_ziptable'
#     'beta.orders'
]

spark.sql("DROP TABLE IF EXISTS grip_platform.weather_forecasts")
spark.sql("DROP TABLE IF EXISTS grip_platform.ziptable")
spark.sql("DROP TABLE IF EXISTS grip_platform.tokens")
spark.sql("DROP TABLE IF EXISTS grip_platform.tokens_permissions")
spark.sql("DROP TABLE IF EXISTS grip_platform.users_customers")
spark.sql("DROP TABLE IF EXISTS grip_platform.users_tokens")

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,ETL Each Table
for t in tables_to_load:
    if t['table_name'] not in skip:
        print(f"Attempting {t}")
        table_name = t['table_name']
        output_folder = t['output_folder']
        output_database = t['output_database']
        select_columns = '*'
        
        if table_name == 'beta.orders' or table_name == 'beta.carrier_ziptable':
            
            id_filter_value = 0
            days_back = 1 ## we could make this lower but for now this gives buffer
            
            while id_filter_value >= 0:
                print(str(table_name), "ID Filter Value: ", str(id_filter_value))                
                query = get_query_for_batch(table_name=table_name, id_filter_value=id_filter_value, batch_size=300000, days_back=days_back)
                results = execute_query(query)
                if len(results) > 0:
                    pandas_df = pd.DataFrame(results, dtype=str)
                    df = spark.createDataFrame(pandas_df)
                    df.createOrReplaceTempView('v_orders_data')

                    spark.sql(f'''
                        MERGE INTO {output_database}.{output_folder} as target
                        USING v_orders_data as source
                        ON target.id = source.id 
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *;
                    ''')
                    
                    id_filter_value = get_query_batch_max_id(df, id_filter_name = 'id')
                else:
                    print('No data to add')
                    id_filter_value = -1
#                     spark.sql(f"OPTIMIZE {output_database}.{output_folder};")
                    print("SUCCESS")
                    print("")
                    
                continue
            continue
                
        if table_name == 'beta.customers':
            select_columns = 'id, guid_id, name, description, folder_path_name, added_datetime, modified_datetime, is_active, is_deleted'
        if table_name == 'beta.integrations':
            select_columns = "id, name, COALESCE(required_fields, '') as required_fields, added_datetime, modified_datetime"
        if table_name == 'beta.shipments':
            select_columns == 'Select id, customer_id,order_id, shipment_guid, shipment_key, warehouse_id, carrier_code_id, carrier_service_code_id,line_haul_id, tracking_number, estimated_ship_date, estimated_tnt, observed_tnt, estimated_delivery_date, properties, shipping_profile_settings_id, refrigerant, products, cartonization_information, box_product_id, weight, sellable_product_type_id, custom_rules, recommendation_configuration_values, grip_rank, fulfillment_warnings, weather_impact, weather_forecast,label_url, batch_id, status, added_datetime, modified_datetime, is_deleted, origin, created_dttm, latest_recommendations, fulfillment_status_id, status_id, _operation_id, _last_modified_by, carrier_status_last_checked_dttm, exception_scan_details, tracking_scan_history, v_shipstation_order_status,CONVERT(datetime, SWITCHOFFSET(out_for_delivery_dttm, RIGHT(out_for_delivery_dttm, 6))) AS out_for_delivery_dttm, CONVERT(datetime, SWITCHOFFSET(delivered_dttm, RIGHT(delivered_dttm, 6))) AS delivered_dttm, CONVERT(datetime, SWITCHOFFSET(exception_dttm, RIGHT(exception_dttm, 6))) AS exception_dttm, CONVERT(datetime, SWITCHOFFSET(shipped_dttm, RIGHT(shipped_dttm, 6))) AS shipped_dttm, CONVERT(datetime, SWITCHOFFSET(label_created_dttm, RIGHT(label_created_dttm, 6))) AS label_created_dttm, CONVERT(datetime, SWITCHOFFSET(label_printed_dttm, RIGHT(label_printed_dttm, 6))) AS label_printed_dttm, CONVERT(datetime, SWITCHOFFSET(canceled_dttm, RIGHT(canceled_dttm, 6))) AS canceled_dttm'
        results = execute_query(f"SELECT {select_columns} FROM {table_name}")
        if len(results) > 0:
            pandas_df = pd.DataFrame(results, dtype=str)
            delta_location = f'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/grip_platform/{output_folder}'
            df = spark.createDataFrame(pandas_df)
            if table_name == 'beta.warehouses_carriers':
                df = df.withColumn('customers_integrations_id', F.col('customers_integrations_id').cast(StringType()))
            df.write.format('delta').option("mergeSchema", "true").mode('overwrite').save(delta_location)
            spark.sql(f"CREATE TABLE IF NOT EXISTS {output_database}.{output_folder} USING DELTA LOCATION '{delta_location}'")
#             print("Optimizing table...")
#             spark.sql(f"OPTIMIZE {output_database}.{output_folder}")
#             print("Optimize success!")
            print("SUCCESS")
            print("")
        else:
            print('No data to add')

# COMMAND ----------

# import json 
# import requests
# import pyspark.sql.functions as F
# print('imports success')

# COMMAND ----------

# def generate_paths(path_prefix, backfill=False):
#     import datetime
#     today = datetime.datetime.now()
#     yesterday = today - datetime.timedelta(days=1)
#     today_string = str(today).replace('-', '/')[:10]
#     yesterday_string = str(yesterday).replace('-', '/')[:10]
#     paths_to_read = []
#     paths_to_read.append(f'{path_prefix}/{today_string}/*.json')
#     paths_to_read.append(f'{path_prefix}/{yesterday_string}/*.json')
#     if backfill:
#         paths_to_read = []
#         paths_to_read.append(f'{path_prefix}/*/*/*/*.json')
        
#     return paths_to_read

# COMMAND ----------

# DBTITLE 1,Get Shipment Info API Requests - TODO: this needs to use new recommendation process
# #raw_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/landing/api_requests/get-shipment-info/*/*/*/*.json'
# raw_path = generate_paths('abfss://gripshipping@gripdatalake.dfs.core.windows.net/landing/api_requests/get-shipment-info', False)
# #checkpoint_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/grip_platform/api_requests/get-shipment-info/_checkpoints'
# #schema_location = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/grip_platform/api_requests/get-shipment-info/'
# #bronze_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/grip_platform/api_requests/get-shipment-info/'
# ## make this a better schema that has all the options for each field; add schema as input 
# # sample_schema = spark.read.json('abfss://gripshipping@gripdatalake.dfs.core.windows.net/landing/api_requests/get-shipment-info/2022/06/*/*.json').schema
# sample_schema = spark.sql("select * from grip_platform.get_shipment_info_api_requests").schema

# # df = (
# #     spark.readStream.format('cloudFiles')
# #     .option('cloudFiles.format', 'json')
# #     .schema(sample_schema)
# #     .option('cloudFiles.schemaLocation', schema_location) 
# #     .load(raw_path)
# # ).withColumn('input_file_name', F.input_file_name())

# df = spark.read.format('json').schema(sample_schema).load(raw_path).withColumn('input_file_name', F.input_file_name())
# df.createOrReplaceTempView("new_requests")

# spark.sql("""
#         MERGE INTO grip_platform.get_shipment_info_api_requests t 
#         USING new_requests s 
#         ON split(t.input_file_name, '/')[9] = split(s.input_file_name, '/')[9]
#         WHEN NOT MATCHED THEN INSERT *
# """).display()

# # (
# #     df.writeStream.format('delta')
# #     .option('checkpointLocation', checkpoint_path)
# #     .option("cloudFiles.schemaEvolutionMode","addNewColumns")
# #     .option("mergeSchema", "true")
# #     .trigger(once=True)
# #     .start(bronze_path)
# #     .awaitTermination()
    
# # )

# COMMAND ----------

# %sql
# OPTIMIZE grip_platform.get_shipment_info_api_requests;

# COMMAND ----------

# DBTITLE 1,ShipStation Sync ETL - TODO: this needs to use new process
# raw_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/landing/api_requests/start-shipstation-sync/*/*/*/*.json'
# checkpoint_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/grip_platform/api_requests/start-shipstation-sync/_checkpoints'
# schema_location = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/grip_platform/api_requests/start-shipstation-sync/'
# bronze_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/grip_platform/api_requests/start-shipstation-sync/'
# ## make this a better schema that has all the options for each field; add schema as input 
# sample_schema = spark.read.json('abfss://gripshipping@gripdatalake.dfs.core.windows.net/landing/api_requests/start-shipstation-sync/2022/08/30/*.json').schema

# df = (
#     spark.readStream.format('cloudFiles')
#     .option('cloudFiles.format', 'json')
#     .schema(sample_schema)
#     .option('cloudFiles.schemaLocation', schema_location) 
#     .load(raw_path)
# ).withColumn('input_file_name', F.input_file_name()).select("input_file_name", "requestParams", "env", "requestTime", "responseDetails.responseText.details")

# (
#     df.writeStream.format('delta')
#     .option('checkpointLocation', checkpoint_path)
#     .option("cloudFiles.schemaEvolutionMode","addNewColumns")
#     .option("mergeSchema", "true")
#     .trigger(once=True)
#     .start(bronze_path)
#     .awaitTermination()
    
# )

# COMMAND ----------

# %sql
# OPTIMIZE grip_platform.shipstation_syncs

# COMMAND ----------

# DBTITLE 1,Get Sessions
# raw_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/landing/api_requests/start-session/*/*/*/*.json'
# checkpoint_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/grip_platform/api_requests/start-session/_checkpoints'
# schema_location = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/grip_platform/api_requests/start-session/'
# bronze_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/grip_platform/api_requests/start-session/'

# df = (
#     spark.readStream.format('cloudFiles')
#     .option('cloudFiles.format', 'json')
#     .option('cloudFiles.schemaLocation', schema_location) 
#     .load(raw_path)
# ).withColumn('input_file_name', F.input_file_name())

# (
#     df.writeStream.format('delta')
#     .option('checkpointLocation', checkpoint_path)
#     .option("cloudFiles.schemaEvolutionMode","addNewColumns")
#     .option("mergeSchema", "true")
#     .trigger(once=True)
#     .start(bronze_path)
#     .awaitTermination()
    
# )

# COMMAND ----------

# DBTITLE 1,Get Failed Requests
# raw_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/landing/api_requests/failed_requests/*/*/*/*/*.json'
# checkpoint_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/grip_platform/api_requests/failed_requests/_checkpoints'
# schema_location = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/grip_platform/api_requests/failed_requests/'
# bronze_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/grip_platform/api_requests/failed_requests/'

# df = (
#     spark.readStream.format('cloudFiles')
#     .option('cloudFiles.format', 'json')
#     .option('cloudFiles.schemaLocation', schema_location) 
#     .load(raw_path)
# ).withColumn('input_file_name', F.input_file_name())

# (
#     df.writeStream.format('delta')
#     .option('checkpointLocation', checkpoint_path)
#     .option("cloudFiles.schemaEvolutionMode","addNewColumns")
#     .option("mergeSchema", "true")
#     .trigger(once=True)
#     .start(bronze_path)
#     .awaitTermination()
    
# )

# COMMAND ----------

# DBTITLE 1,Create raw bronze table from this for recommendation history TODO: make this faster
raw_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/landing/_v2/request_logging/*/RECOMMENDATION_REQUEST/*/*/*/*/*.json'
checkpoint_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/_v2/grip_platform/request_logging/RECOMMENDATION_REQUEST/_checkpoints'
schema_location = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/_v2/grip_platform/request_logging/RECOMMENDATION_REQUEST/'
schema_ = "orders_requested string, orders string, profile_id string, use_profile string, config string, features string, async_origin string, timestamp string"
bronze_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/_v2/grip_platform/request_logging/RECOMMENDATION_REQUEST/'
silver_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/silver/_v2/grip_platform/request_logging/RECOMMENDATION_REQUEST/'

df = (
    spark.readStream.format('cloudFiles')
    .schema(schema_)
    .option('cloudFiles.format', 'json')
    .option('cloudFiles.schemaLocation', schema_location) 
    .load(raw_path)
    .withColumn('input_file_name', F.input_file_name())
    .withColumn('grip_processed_time', F.lit(datetime.datetime.now()))
)

(
    df.writeStream.format('delta')
    .option('checkpointLocation', checkpoint_path)
    .option("cloudFiles.schemaEvolutionMode","addNewColumns")
    .option("mergeSchema", "true")
    .trigger(once=True)
    .start(bronze_path)
    .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE grip_platform.order_recommendation_history_bronze

# COMMAND ----------

# DBTITLE 1,Transformation Testing - TODO: make this incremental
import json 
silver_df = (
    spark.sql('''
        SELECT
            orders,  
            profile_id,
            use_profile,
            config,
            features,
            async_origin,
            cast(timestamp as timestamp) as timestamp,
            input_file_name,
            split(input_file_name, '/')[6] as customer_id,
            split(input_file_name, '/')[8] as request_type,
            split(input_file_name, '/')[size(split(input_file_name, '/')) - 1] as request_id,
            grip_processed_time
        FROM grip_platform.order_recommendation_history_bronze
    ''')
    .withColumn("order_recommendations_array", F.from_json('orders',
                                               '''
                                                   array<struct<
                                                       box_insulated:string,
                                                        box_insulation_thickness_inches:string,
                                                        box_internal_dimensions:string,
                                                        box_sku:string,
                                                        box_volume:string,
                                                        box_volume_usable:string,
                                                        box_volume_usable_reduction_factor:string,
                                                        carrier_code:string,
                                                        carrier_service_code:string,
                                                        carrier_time_in_transit:string,
                                                        custom_rule_applied:string,
                                                        destination_postal_code:string,
                                                        dry_ice_volume:string,
                                                        estimated_delivery_date:string,
                                                        estimated_ship_date:string,
                                                        grip_rank:string,
                                                        line_haul_code:string,
                                                        max_temp_f:string,
                                                        order_box_id:string,
                                                        order_id:string,
                                                        order_perishability_id:string,
                                                        order_rules_type:string,
                                                        order_weight:string,
                                                        origin_postal_code:string,
                                                        product_dry_ice_volume:string,
                                                        product_volume:string,
                                                        products:string,
                                                        products_unfulfillable_by_facility_total:string,
                                                        profile_settings_id:string,
                                                        recommendation_configuration_values:string,
                                                        recommended_only:string,
                                                        refrigerant:string,
                                                        saturday_delivery:string,
                                                        sunday_delivery:string,
                                                        warehouse_code:string,
                                                        warehouse_customer_internal_id:string,
                                                        weather_impact:string,
                                                        weather_impact_total:string
                                                       >>
                                               ''')
               )
    .selectExpr(
        'explode(order_recommendations_array) as order_recommendation',
         'profile_id',
         'use_profile',
         'config',
         'features',
         'async_origin',
         'timestamp',
         'input_file_name',
         'request_id',
         'customer_id',
         'request_type',
         'grip_processed_time'
    )
)

silver_path = 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/silver/_v2/grip_platform/request_logging/RECOMMENDATION_REQUEST/'

silver_df.write.format('delta').option('mergeSchema', 'true').mode('overwrite').save(silver_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE grip_platform.order_recommendation_history_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE TABLE IF NOT EXISTS grip_platform.order_recommendation_history_bronze USING DELTA LOCATION 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/bronze/_v2/grip_platform/request_logging/RECOMMENDATION_REQUEST/'
# MAGIC --CREATE TABLE IF NOT EXISTS grip_platform.order_recommendation_history_silver USING DELTA LOCATION 'abfss://gripshipping@gripdatalake.dfs.core.windows.net/silver/_v2/grip_platform/request_logging/RECOMMENDATION_REQUEST/'
