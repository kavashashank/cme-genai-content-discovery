# Databricks notebook source
catalog_name = 'cme_genai_dev'
schema_name = 'content_discovery'
volume_path = 's3://one-env-uc-external-location/aaboode/skava/content_discovery_data'
volume_name = 'content_discovery_vol'

dbutils.widgets.text('Catalog', catalog_name, 'Catalog Name')
dbutils.widgets.text('Schema', schema_name, 'Schema Name')
#dbutils.widgets.text('Volume Path', volume_path, 'Volume Path')
dbutils.widgets.text('Volume Name', volume_name, 'Volume Name') 
dbutils.widgets.combobox('reset_all', 'false', ['true', 'false'], 'Reset all existing data')

# COMMAND ----------

import pyspark.sql.functions as F
catalog = dbutils.widgets.get('Catalog')
schema = dbutils.widgets.get('Schema')
#volume_path = dbutils.widgets.get('Volume Path')
volume_name = dbutils.widgets.get('Volume Name')
reset_all = dbutils.widgets.get('reset_all') == "true"


# COMMAND ----------

print(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
print(f"USE CATALOG `{catalog}`")
spark.sql(f"USE CATALOG `{catalog}`")
print(f"CREATE DATABASE `{schema}`")
spark.sql(f"""CREATE DATABASE IF NOT EXISTS `{schema}` """)
print(f"USE DATABASE `{schema}`")
spark.sql(f"""USE DATABASE `{schema}` """)

print(f"CREATE VOLUME IF NOT EXISTS `{volume_path}`")
spark.sql(f"""CREATE VOLUME IF NOT EXISTS `{volume_name}`""")

if reset_all:
  print(f"DROP DATABASE `{schema}`")
  spark.sql(f"DROP DATABASE IF EXISTS `{schema}` CASCADE")

# COMMAND ----------

workspace_url = "https://" + spark.conf.get("spark.databricks.workspaceUrl") 
base_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

# COMMAND ----------

#DATA PREP
chunk_size=200
chunk_overlap=50

sync_table_name = 'video_transcripts_for_sync'

# COMMAND ----------

secrets_scope='shovakeemian-scope'
secrets_hf_key_name='hugging-face-key'

# COMMAND ----------

#EMBEDDING MODEL
embedding_model_name='shovakeemian-e5-small-v2'
registered_embedding_model_name = f'{catalog}.{schema}.{embedding_model_name}'
embedding_endpoint_name = 'shovakeemian-e5-small-v2'

# COMMAND ----------

#VECTOR SEARCH
vs_endpoint_name='shared_demo_endpoint'
vs_index = 'video_transcript_index'

vs_index_fullname = f"{catalog}.{schema}.{vs_index}"
sync_table_fullname = f"{catalog}.{schema}.{sync_table_name}"

# COMMAND ----------

#LLM SERVING
llm_model_name=''
registered_llm_model_name=f'{catalog}.{schema}.{llm_model_name}'
llm_endpoint_name = ''
