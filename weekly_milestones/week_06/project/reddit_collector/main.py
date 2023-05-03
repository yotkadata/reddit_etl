"""
This script gets reddits from the reddit api 
and inserts them to a MongoDB.
"""

import datetime as dt

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

    print("Get temporary access token.")
    response = requests.post(
        url=url, headers=auth_info["headers"], data=data, auth=basic_auth
    ).json()
    print("Done.\n")

    return response["token_type"], response["access_token"]


def get_reddits(auth_info, topic="Berlin"):
    """
    Function to get Reddit posts using the API.
    """

    print("Start downloading posts form Reddit via API.\n")

    # Get access token and add it to header
    token_type, access_token = auth_get_token(auth_info)
    conf["headers"]["Authorization"] = token_type + " " + access_token

    # Send a get request to download the latest (new) subreddits using the new headers.
    url = f"https://oauth.reddit.com/r/{topic}/new"  # You could also select ".../hot" to fetch the most popular posts.

    # Start GET request
    print("Starting GET request")
    response = requests.get(
        url=url,
        headers=conf["headers"],
        params={"limit": 5},
    ).json()

    print(f"Done. Received {len(response['data']['children'])} posts.\n")

    return response


def write_to_mongodb(full_response):
    """
    Function to write posts to MongoDB
    """

    print("Create connection to MongoDB.\n")

    # Create a connection to the MongoDB database server
    client = pymongo.MongoClient(host="mongodb")

    # Create/use a database
    db = client.reddit

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
            "date": post_time,
            "title": post["data"]["title"],
            "text": post["data"]["selftext"],
        }

        # Insert the post into the collection (like INSERT INTO posts VALUES (....);)
        print("----- Writing post into MongoDB -----")
        print(str(dt.datetime.now()))
        print(f"{data['title'][:101]} (ID: {key['_id']})")

        res = collection.update_one(key, {"$set": data}, upsert=True)

        if res.matched_count > 0:
            print(
                f"Post already existed in MongoDB. {'Modified.' * res.modified_count}"
            )

        print("-------------------------------------\n")

    print(f"Added {len(full_response)} posts to MongoDB.")


def main():
    reddits = get_reddits(conf, topic="Berlin")
    write_to_mongodb(reddits["data"]["children"])


if __name__ == "__main__":
    main()
