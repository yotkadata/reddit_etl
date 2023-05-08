import os
import re
import time

import pymongo
from sqlalchemy import create_engine, text
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Get environment variables
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE")


def clean_corpus(text):
    """
    Function to clean the corpus.
    """

    # Remove apostrophs
    text = text.replace("'", " ")
    # Remove line breaks
    text = text.replace("\n", "")
    # Remove URLs
    text = re.sub(r"\S*https?:\S*", "", text)
    # Remove HTML tags
    text = re.sub(re.compile("<.*?>"), "", text)

    return text


def extract():
    """
    Function to extract Reddits from MongoDB.
    """

    # Take a moment to wait for the reddit collector to finish
    time.sleep(5)

    # Establish a connection to the MongoDB server
    client = pymongo.MongoClient(host="mongodb", port=27017)

    # Select the database you want to use withing the MongoDB server
    db = client.reddit_posts

    # Get all posts and return them
    posts = list(db.posts.find())

    return posts


def transform(posts):
    """
    Function to transform the data.
    """

    # Instanciate sentiment analyzer
    s = SentimentIntensityAnalyzer()

    for post in posts:
        post["text"] = clean_corpus(post["text"])

        # Calculate sentiment intensity
        sentiment = s.polarity_scores(post["text"])
        post["score"] = sentiment["compound"]

    return posts


def load(posts):
    """
    Function to write the transformed data to Postgres DB.
    """

    # Create a postgres client
    pg_client = create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgresdb:5432/{POSTGRES_DB}",
        echo=True,
    )

    # Connect the client to postgres
    pg_client_connect = pg_client.connect()

    # Construct SQL query to create a table
    create_table = text(
        """
        CREATE TABLE IF NOT EXISTS posts (
            id VARCHAR(32) PRIMARY KEY,
            date TIMESTAMP,
            sub_id VARCHAR(32),
            subreddit VARCHAR(32),
            author_id VARCHAR(32),
            author VARCHAR(32),
            title TEXT,
            text TEXT,
            url VARCHAR(250),
            upvote_ratio NUMERIC,
            num_comments INT,
            sentiment NUMERIC,
            slacked INT
        );
        """
    )

    # Execute the query create_table
    pg_client_connect.execute(create_table)
    pg_client_connect.commit()

    # Insert posts into DB
    for post in posts:
        insert = text(
            """
            INSERT INTO posts (
                id, date, sub_id, subreddit, author_id, author, title, 
                text, url, upvote_ratio, num_comments, sentiment, slacked
                ) 
            VALUES (
                :id, :date, :sub_id, :subreddit, :author_id, :author, :title, 
                :text, :url, :upvote_ratio, :num_comments, :sentiment, :slacked
                )
            ON CONFLICT (id) DO UPDATE 
                SET date = excluded.date,
                    sub_id = excluded.sub_id,
                    subreddit = excluded.subreddit,
                    author_id = excluded.author_id,
                    author = excluded.author,
                    title = excluded.title,
                    text = excluded.text,
                    url = excluded.url,
                    upvote_ratio = excluded.upvote_ratio,
                    num_comments = excluded.num_comments,
                    sentiment = excluded.sentiment
            """
        )

        # Execute the query insert
        pg_client_connect.execute(
            insert,
            {
                "id": post["_id"],
                "date": post["date"],
                "sub_id": post["sub_id"],
                "subreddit": post["subreddit"],
                "author_id": post["author_id"],
                "author": post["author"],
                "title": post["title"],
                "text": post["text"],
                "url": post["url"],
                "upvote_ratio": post["upvote_ratio"],
                "num_comments": post["num_comments"],
                "sentiment": post["score"],
                "slacked": 0,
            },
        )
        pg_client_connect.commit()


def main():
    # Wait for the other jobs to finish
    time.sleep(15)

    load(transform(extract()))


if __name__ == "__main__":
    main()
