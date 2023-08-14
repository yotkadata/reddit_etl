"""
This script gets reddits from the reddit api 
and inserts them to a MongoDB.
"""

import datetime as dt
import logging
import os

import pymongo
import requests
from requests.auth import HTTPBasicAuth

# Get environment variables
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_SECRET = os.getenv("REDDIT_SECRET")
REDDIT_USER = os.getenv("REDDIT_USER")
REDDIT_PWD = os.getenv("REDDIT_PWD")

# Set headers
REDDIT_HEADERS = {"User-Agent": "TestAppforDocker"}


def auth_get_token() -> tuple[str, str]:
    """
    Function to request a temporary access token.
    """

    basic_auth = HTTPBasicAuth(
        username=REDDIT_CLIENT_ID,  # Reddit client_id
        password=REDDIT_SECRET,  # Reddit "secret"
    )

    data = {
        "grant_type": "password",
        "username": REDDIT_USER,  # Reddit username
        "password": REDDIT_PWD,  # Reddit password
    }

    # Start POST request for access token
    url = "https://www.reddit.com/api/v1/access_token"

    logging.info("Get temporary access token.")
    response = requests.post(
        url=url, headers=REDDIT_HEADERS, data=data, auth=basic_auth, timeout=30
    ).json()
    logging.info("Done.\n")

    return response["token_type"], response["access_token"]


def get_reddits(topics: list = None, num_posts: int = 25) -> list:
    """
    Function to get Reddit posts using the API.
    """

    responses = []

    if not topics:
        logging.critical("No topics provided.")
        return responses

    # Convert to list if topic is string
    if isinstance(topics, str):
        topics = [topics]

    for topic in topics:
        logging.info("Start downloading reddits for topic %s\n", topic)

        # Get access token and add it to header
        token_type, access_token = auth_get_token()
        REDDIT_HEADERS["Authorization"] = token_type + " " + access_token

        # Send a get request to download the latest (new) subreddits using the new headers.
        url = f"https://oauth.reddit.com/r/{topic}/new"  # You could also select ".../hot" to fetch the most popular posts.

        # Start GET request
        logging.info("Starting GET request")
        response = requests.get(
            url=url,
            headers=REDDIT_HEADERS,
            params={"limit": num_posts},
            timeout=30,
        ).json()

        logging.info("Done. Received %s posts.\n", len(response["data"]["children"]))

        responses.append(response)

    return responses


def write_to_mongodb(full_response: list) -> None:
    """
    Function to write posts to MongoDB
    """

    logging.info("Create connection to MongoDB.\n")

    # Create a connection to the MongoDB database server
    client = pymongo.MongoClient(host="mongodb")

    # Create/use a database
    database = client.reddit_posts

    # Define the collection (like a table)
    collection = database.posts

    for post in full_response:
        # Convert date
        post_time = post["data"]["created_utc"]
        post_time = dt.datetime.fromtimestamp(post_time).strftime("%Y-%m-%d %H:%M:%S")

        # Prepare input to MongoDB
        key = {"_id": post["data"]["id"]}
        data = {
            "sub_id": post["data"]["subreddit_id"],
            "subreddit": post["data"]["subreddit"],
            "date": post_time,
            "title": post["data"]["title"],
            "text": post["data"]["selftext"],
            "author_id": post["data"]["author_fullname"],
            "author": post["data"]["author"],
            "url": post["data"]["url"],
            "upvote_ratio": post["data"]["upvote_ratio"],
            "num_comments": post["data"]["num_comments"],
        }

        # Insert the post into the collection (like INSERT INTO posts VALUES (....);)
        logging.info("----- Writing post into MongoDB -----")
        logging.info(str(dt.datetime.now()))
        logging.info("%s (ID: %s)", data["title"][:101], key["_id"])

        res = collection.update_one(key, {"$set": data}, upsert=True)

        if res.matched_count > 0:
            logging.info(
                "Post already existed in MongoDB. %s", "Modified." * res.modified_count
            )

        logging.info("-------------------------------------\n")

    logging.info("Added %s posts to MongoDB.", len(full_response))


def main() -> None:
    """
    Main function to get reddits and write them to MongoDB.
    """
    reddits = get_reddits(
        topics=["datascience", "artificialinteligence", "dataanalysis", "python"],
        num_posts=100,
    )

    for reddit in reddits:
        write_to_mongodb(reddit["data"]["children"])


if __name__ == "__main__":
    main()
