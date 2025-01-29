from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, split, explode, count, lit, date_format
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import pandas as pd

spark = SparkSession.builder \
    .appName("ArticlesAnalysis") \
    .config("spark.local.dir", "C:\\spark_dir") \
    .getOrCreate()

# Load JSON file
input_path = "articles.json"
articles_df = spark.read.option("multiline", "true").json(input_path)

# Normalize and clean text for processing
def clean_text_column(df, col_name):
    return df.withColumn(col_name, lower(regexp_replace(col(col_name), "[\\W_]+", " ")))

articles_df = clean_text_column(articles_df, "summary")
articles_df = clean_text_column(articles_df, "title")

# Extract keywords by splitting text into words
def extract_keywords(df, col_name):
    return df.withColumn("keywords", split(col(col_name), " "))

articles_df = extract_keywords(articles_df, "summary")

# Identify entities like companies, persons, and places (using rule-based methods)
def identify_entities(df):
    companies = ["spacex", "boeing", "nasa", "blue origin"]
    persons = ["elon musk", "jeff bezos", "richard branson"]
    places = ["earth", "mars", "international space station"]

    def match_entities(entities_list, column):
        return col(column).rlike("|".join([f"\\b{entity}\\b" for entity in entities_list]))

    df = df.withColumn("entities_companies", match_entities(companies, "summary")) \
           .withColumn("entities_persons", match_entities(persons, "summary")) \
           .withColumn("entities_places", match_entities(places, "summary"))

    return df

articles_df = identify_entities(articles_df)

# Classify articles by topics based on keywords (rule-based classification)
def classify_articles(df):
    topics = {
        "space exploration": ["mars", "moon", "exploration", "orbit"],
        "technology": ["satellite", "innovation", "ai", "robotics"],
        "companies": ["spacex", "boeing", "nasa", "blue origin"],
    }

    for topic, keywords in topics.items():
        df = df.withColumn(
            f"is_{topic.replace(' ', '_')}",
            col("summary").rlike("|".join([f"\\b{keyword}\\b" for keyword in keywords])).cast("boolean")
        )

    return df

articles_df = classify_articles(articles_df)

# Tendency analysis: Analyze topics by time and active news sources
def analyze_tendencies(df):
    df = df.withColumn("published_date", date_format(col("published_at"), "yyyy-MM-dd"))

    # Count topics by time
    topic_counts = df.select("published_date", "is_space_exploration", "is_technology", "is_companies")
    topic_counts = topic_counts.groupBy("published_date").agg(
        count("is_space_exploration").alias("space_exploration_count"),
        count("is_technology").alias("technology_count"),
        count("is_companies").alias("companies_count")
    )

    # Analyze active news sources
    source_activity = df.groupBy("news_site").agg(count("id").alias("articles_count"))

    return topic_counts, source_activity

topic_counts_df, source_activity_df = analyze_tendencies(articles_df)

# Convert Spark DataFrames to Pandas DataFrames
articles_pd = articles_df.toPandas()
topic_counts_pd = topic_counts_df.toPandas()
source_activity_pd = source_activity_df.toPandas()

# Save processed data and analysis results to JSON using Pandas
output_articles_path = "processed_articles.json"
output_topics_path = "topic_tendencies.json"
output_sources_path = "source_activity.json"

articles_pd.to_json(output_articles_path, orient="records", indent=4)
topic_counts_pd.to_json(output_topics_path, orient="records", indent=4)
source_activity_pd.to_json(output_sources_path, orient="records", indent=4)

print("Processing complete. Results saved to JSON files.")

# Stop Spark session
spark.stop()



