import re
import time

import pymongo
from sqlalchemy import create_engine, text


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

    # Take a moment to wait for MongoDB
    time.sleep(10)

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

    for post in posts:
        post["text"] = clean_corpus(post["text"])
        post["score"] = 1.0  # Placeholder

    return posts


def load(posts):
    """
    Function to write the transformed data to Postgres DB.
    """

    # Create a postgres client
    pg_client = create_engine(
        "postgresql://postgres:12345678@postgresdb:5432/reddit_posts", echo=True
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
        author_id VARCHAR(32),
        author VARCHAR(32),
        title TEXT,
        text TEXT,
        url VARCHAR(250),
        upvote_ratio NUMERIC,
        num_comments INT,
        sentiment NUMERIC
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
                    id, date, sub_id, author_id, author, title, text, url, upvote_ratio, num_comments, sentiment
                    ) 
                VALUES (
                    :id, :date, :sub_id, :author_id, :author, :title, :text, :url, :upvote_ratio, :num_comments, :sentiment
                    )
                ON CONFLICT (id) DO UPDATE 
                    SET date = excluded.date,
                        sub_id = excluded.sub_id,
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
                "author_id": post["author_id"],
                "author": post["author"],
                "title": post["title"],
                "text": post["text"],
                "url": post["url"],
                "upvote_ratio": post["upvote_ratio"],
                "num_comments": post["num_comments"],
                "sentiment": post["score"],
            },
        )
        pg_client_connect.commit()


def main():
    load(transform(extract()))


if __name__ == "__main__":
    main()
