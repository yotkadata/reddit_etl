import time

import requests
import sqlalchemy as db
from config import conf


def pg_connect():
    """
    Function to create a connection to Postgres
    """

    # Create a postgres client
    pg_client = db.create_engine(
        f"postgresql://{conf['postgres_user']}:{conf['postgres_password']}@{conf['postgres_db']}:5432/{conf['postgres_table']}",
        echo=True,
    )

    # Connect the client to postgres
    pg_client_connect = pg_client.connect()

    return pg_client_connect


def load_last_sentiment_post():
    """
    Function to get posts from Postgres.
    """

    pg_client_connect = pg_connect()

    # Select the last unsent Reddit with highest sentiment
    query = db.text(
        """
        SELECT id, subreddit, title, date, sentiment, url, author 
        FROM posts WHERE slacked = 0 
        ORDER BY ABS(sentiment) DESC 
        LIMIT 1;
        """
    )

    posts = pg_client_connect.execute(query).mappings().all()

    return posts[0]


def prepare_slack_message(post):
    """
    Function to format a message for Slack.
    """

    # Define an emoji depending on the sentiment
    emoji = ":smiley:" if post["sentiment"] > 0 else ":face_with_symbols_on_mouth:"

    slack_message = f"""
        There is something spicy :chili: going on in this Reddit (sentiment: {post["sentiment"]} {emoji}):

        *{post["title"]}*

        _Published on {post["date"]} by {post["author"]}_
        {post["url"]}
        """

    return slack_message


def send_slack_message(message, webhook_url):
    """
    Function to send a Slack message.
    """

    res = requests.post(url=webhook_url, json={"text": message})

    if res.status_code == 200:
        return True

    return False


def set_slacked(post_id):
    """
    Function to set a Reddit as 'slacked' after sending it.
    """

    pg_client_connect = pg_connect()

    query = db.text(
        """
        UPDATE posts
        SET slacked = 1
        WHERE id = :id;
        """
    )

    pg_client_connect.execute(query, {"id": post_id})
    pg_client_connect.commit()


def main():
    # Wait for the other jobs to finish
    time.sleep(10)

    # Get posts from Postgres
    post = load_last_sentiment_post()

    # Prepare the slack message
    slack_message = prepare_slack_message(post)

    # Send it
    message_sent = send_slack_message(slack_message, conf["webhook_url"])

    # Set sent status
    if message_sent:
        set_slacked(post["id"])


if __name__ == "__main__":
    main()
