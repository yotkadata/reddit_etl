"""
This script gets reddits from the reddit api 
and inserts them to a MongoDB.
"""

import datetime as dt
import logging

import pymongo
import requests
from config import conf
from requests.auth import HTTPBasicAuth


def auth_get_token(auth_info):
    """
    Function to request a temporary access token.
    """

    basic_auth = HTTPBasicAuth(
        username=auth_info["client_id"],  # Reddit client_id
        password=auth_info["secret"],  # Reddit "secret"
    )

    data = dict(
        grant_type="password",
        username=auth_info["username"],  # Reddit username
        password=auth_info["password"],  # Reddit password
    )

    # Start POST request for access token
    url = "https://www.reddit.com/api/v1/access_token"

    logging.info("Get temporary access token.")
    response = requests.post(
        url=url, headers=auth_info["headers"], data=data, auth=basic_auth
    ).json()
    logging.info("Done.\n")

    return response["token_type"], response["access_token"]


def get_reddits(auth_info, topics=None, num_posts=25):
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
        logging.info(f"Start downloading reddits for topic {topic}\n")

        # Get access token and add it to header
        token_type, access_token = auth_get_token(auth_info)
        conf["headers"]["Authorization"] = token_type + " " + access_token

        # Send a get request to download the latest (new) subreddits using the new headers.
        url = f"https://oauth.reddit.com/r/{topic}/new"  # You could also select ".../hot" to fetch the most popular posts.

        # Start GET request
        logging.info("Starting GET request")
        response = requests.get(
            url=url,
            headers=conf["headers"],
            params={"limit": num_posts},
        ).json()

        logging.info(f"Done. Received {len(response['data']['children'])} posts.\n")

        responses.append(response)

    return responses


def write_to_mongodb(full_response):
    """
    Function to write posts to MongoDB
    """

    logging.info("Create connection to MongoDB.\n")

    # Create a connection to the MongoDB database server
    client = pymongo.MongoClient(host="mongodb")

    # Create/use a database
    db = client.reddit_posts

    # Define the collection (like a table)
    collection = db.posts

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
        logging.info(f"{data['title'][:101]} (ID: {key['_id']})")

        res = collection.update_one(key, {"$set": data}, upsert=True)

        if res.matched_count > 0:
            logging.info(
                f"Post already existed in MongoDB. {'Modified.' * res.modified_count}"
            )

        logging.info("-------------------------------------\n")

    logging.info(f"Added {len(full_response)} posts to MongoDB.")


def main():
    reddits = get_reddits(
        conf,
        topics=["datascience", "artificialinteligence", "dataanalysis", "python"],
        num_posts=100,
    )

    for reddit in reddits:
        write_to_mongodb(reddit["data"]["children"])


if __name__ == "__main__":
    main()
