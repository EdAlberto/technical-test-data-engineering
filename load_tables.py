import json
import asyncpg
import asyncio
from datetime import datetime

# Load JSON files
articles_file = "processed_articles.json"
topics_file = "topic_tendencies.json"
sources_file = "source_activity.json"

with open(articles_file, "r") as f:
    articles_data = json.load(f)

with open(topics_file, "r") as f:
    topics_data = json.load(f)

with open(sources_file, "r") as f:
    sources_data = json.load(f)

# Define the dimensional model
create_dim_topics_query = """
CREATE TABLE IF NOT EXISTS dim_topics (
    id SERIAL PRIMARY KEY,
    topic_name TEXT UNIQUE NOT NULL
);
"""

create_dim_sources_query = """
CREATE TABLE IF NOT EXISTS dim_sources (
    id SERIAL PRIMARY KEY,
    source_name TEXT UNIQUE NOT NULL
);
"""

create_fact_articles_query = """
CREATE TABLE IF NOT EXISTS fact_articles (
    id SERIAL,
    article_id INT NOT NULL,
    title TEXT NOT NULL,
    published_date DATE NOT NULL,
    topic_id INT REFERENCES dim_topics(id),
    source_id INT REFERENCES dim_sources(id),
    keywords TEXT[],
    entities TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, published_date)
) PARTITION BY RANGE (published_date);
"""

# Define partition for fact_articles
create_partition_query = """
CREATE TABLE IF NOT EXISTS fact_articles_y{year}m{month}
PARTITION OF fact_articles
FOR VALUES FROM ('{year}-{month}-01') TO ('{year}-{month}-31');
"""

# Insert data into dimensions
insert_dim_topics_query = """
INSERT INTO dim_topics (topic_name) VALUES ($1)
ON CONFLICT (topic_name) DO NOTHING
RETURNING id;
"""

insert_dim_sources_query = """
INSERT INTO dim_sources (source_name) VALUES ($1)
ON CONFLICT (source_name) DO NOTHING
RETURNING id;
"""

# Insert data into fact_articles
insert_fact_articles_query = """
INSERT INTO fact_articles (
    article_id, title, published_date, topic_id, source_id, keywords, entities
) VALUES ($1, $2, $3, $4, $5, $6, $7)
"""

async def load_data():
    # Connect to the PostgreSQL database
    connection = await asyncpg.connect(
        user="user",
        password="password",
        database="postgres",
        host="host",
        port="port"
    )

    # Create tables
    await connection.execute(create_dim_topics_query)
    await connection.execute(create_dim_sources_query)
    await connection.execute(create_fact_articles_query)

    # Create partitions dynamically based on article dates
    for article in articles_data:
        date_str = article.get("published_at")
        if date_str:
            date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
            partition_query = create_partition_query.format(year=date.year, month=f"{date.month:02d}")
            await connection.execute(partition_query)

    # Insert topics and sources into dimensions
    topic_id_map = {}
    for topic in topics_data:
        result = await connection.fetchval(insert_dim_topics_query, f"space exploration")
        if result:
            topic_id_map[f"space exploration"] = result

    source_id_map = {}
    for source in sources_data:
        result = await connection.fetchval(insert_dim_sources_query, source["news_site"])
        if result:
            source_id_map[source["news_site"]] = result

    # Insert articles into fact_articles
    for article in articles_data:
        published_date = datetime.strptime(article["published_at"], "%Y-%m-%dT%H:%M:%SZ") if article.get("published_at") else None
        topic_id = topic_id_map.get("space exploration")  # Assuming space exploration as a topic
        source_id = source_id_map.get(article.get("news_site"))

        entities = []
        if article.get("entities_companies"):
            entities.append("company")
        if article.get("entities_persons"):
            entities.append("person")
        if article.get("entities_places"):
            entities.append("place")

        await connection.execute(
            insert_fact_articles_query,
            article["id"],
            article["title"],
            published_date,
            topic_id,
            source_id,
            article.get("keywords"),
            entities
        )

    # Close the connection
    await connection.close()
    print("Data has been successfully loaded into the dimensional model.")

# Run the asynchronous function
asyncio.run(load_data())

