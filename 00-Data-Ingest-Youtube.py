# Databricks notebook source
# MAGIC %md
# MAGIC #Install packages for loading transcripts

# COMMAND ----------

# MAGIC %pip install langchain_community;

# COMMAND ----------

# MAGIC %pip install --upgrade --quiet  youtube-transcript-api

# COMMAND ----------

# MAGIC %pip install --upgrade --quiet  pytube

# COMMAND ----------

#Imports for the Youtube Transcript API
from langchain_community.document_loaders.youtube import TranscriptFormat
from langchain_community.document_loaders import YoutubeLoader


from langchain_core.documents.base import Document
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
import json
from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md 
# MAGIC #Function definitions
# MAGIC

# COMMAND ----------


def get_transcript(yt_video_url):

  loader_content = YoutubeLoader.from_youtube_url(
      yt_video_url,
      add_video_info=True,
      transcript_format=TranscriptFormat.CHUNKS,
      chunk_size_seconds=120,
  )

  loader_id = YoutubeLoader.extract_video_id(yt_video_url)

  return (loader_content.load(), loader_id)

# COMMAND ----------


def parse_document_chunks(document_chunks, youtube_id):

  # Define the schema for the DataFrame
  schema = StructType([
      StructField("page_content", StringType(), True),
      StructField("source", StringType(), True),
      StructField("title", StringType(), True),
      StructField("description", StringType(), True),
      StructField("view_count", LongType(), True),
      StructField("thumbnail_url", StringType(), True),
      StructField("publish_date", StringType(), True),
      StructField("length", LongType(), True),
      StructField("author", StringType(), True),
      StructField("start_seconds", LongType(), True),
      StructField("start_timestamp", StringType(), True)
  ])

  # Convert the list of Document objects to a list of dictionaries
  document_dicts = [
      {
          "page_content": doc.page_content,
          "source": doc.metadata.get("source"),
          "title": doc.metadata.get("title"),
          "description": doc.metadata.get("description"),
          "view_count": doc.metadata.get("view_count"),
          "thumbnail_url": doc.metadata.get("thumbnail_url"),
          "publish_date": doc.metadata.get("publish_date"),
          "length": doc.metadata.get("length"),
          "author": doc.metadata.get("author"),
          "start_seconds": doc.metadata.get("start_seconds"),
          "start_timestamp": doc.metadata.get("start_timestamp")
      }
      for doc in document_chunks
  ]

  # Create a Spark DataFrame from the list of dictionaries
  document_chunks_df = spark.createDataFrame(document_dicts, schema)
  document_chunks_df= document_chunks_df.withColumn("youtube_id", lit(youtube_id))

  return document_chunks_df


# COMMAND ----------


# Append the DataFrame to the Delta table
def append_to_delta(df, table_name):
  df.write.mode("append").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Clean up bronze table before re-ingesting

# COMMAND ----------

# MAGIC %sql
# MAGIC --DELETE FROM cme_genai_dev.content_discovery.parsed_transcript_bronze;

# COMMAND ----------

# MAGIC %md 
# MAGIC #Load transcript chunks in bronze tables for the Youtube links

# COMMAND ----------


urls=["https://www.youtube.com/watch?v=UfbyzK488Hk", "https://www.youtube.com/watch?v=H8Bd62D0MOI&list=PLTPXxbhUt-YVM-AM309uyZrcMwfLE4se2","https://www.youtube.com/watch?v=VzVIT1yPdnw&list=PLTPXxbhUt-YVM-AM309uyZrcMwfLE4se2"]

catalog = "cme_genai_dev"
db = "content_discovery"
bronze_transcripts_table_name = "parsed_transcript_bronze"
full_bronze_transcripts_table_name = catalog+"."+db+"."+bronze_transcripts_table_name

for yt_video_url in urls:
  print ("\nVideo URL being processed: ",yt_video_url)

  # Get the transcript for a Youtube Video
  document_chunks, youtube_id = get_transcript(yt_video_url);

  # Parse the transcript into a Spark DataFrame
  parsed_document_df = parse_document_chunks(document_chunks, youtube_id);

  # Append the DataFrame to the Delta table
  append_to_delta(parsed_document_df,full_bronze_transcripts_table_name);

  print ("Video URL done processing: ",yt_video_url,", Youtube ID:",youtube_id);

# COMMAND ----------

# MAGIC %md 
# MAGIC # Display the ingested bronze data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT youtube_id, * FROM cme_genai_dev.content_discovery.parsed_transcript_bronze;
