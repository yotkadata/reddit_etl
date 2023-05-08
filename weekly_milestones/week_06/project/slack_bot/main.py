import logging
import os
import time

import requests
import sqlalchemy as db

# Get environment variables
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


def pg_connect():
    """
    Function to create a connection to Postgres
    """

    # Create a postgres client
    pg_client = db.create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgresdb:5432/{POSTGRES_DB}",
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
        f"""
        SELECT id, subreddit, title, date, sentiment, url, author 
        FROM {POSTGRES_TABLE} WHERE slacked = 0 
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
        There is something spicy going on in this Reddit (sentiment: {post["sentiment"]} {emoji}):

        *{post["title"]}*

        _Published on {post["date"]} by {post["author"]}_
        {post["url"]}
        """

    return slack_message


def load_sentiment_list(positive=True, num_posts=5):
    """
    Function to get list of posts from Postgres.
    """

    pg_client_connect = pg_connect()

    desc = "DESC" if positive else ""

    # Select the last n Reddits with highest/lowest sentiment
    query = db.text(
        f"""
        SELECT id, author, subreddit, title, date, sentiment, url 
        FROM {POSTGRES_TABLE} 
        WHERE date > (NOW() - INTERVAL '1 DAY') 
        ORDER BY sentiment {desc} LIMIT {num_posts}
        """
    )

    posts = pg_client_connect.execute(query).mappings().all()

    return posts


def prepare_slack_message_list(positive=True):
    """
    Function to format a message with a list for Slack.
    """

    posts = load_sentiment_list(positive, num_posts=5)

    # If no posts, return empty string
    if len(posts) == 0:
        return ""

    message_list = []
    subreddits = []

    for post in posts:
        m = (
            f"*<{post['url']}|{post['title']}>*"
            + "\n"
            + f"_*Score: {post['sentiment']}*, published on {post['date']} by {post['author']} in r/{post['subreddit']}_"
            + "\n\n"
        )
        message_list.append(m.lstrip())
        subreddits.append(post["subreddit"])

    subreddits = ", ".join(list(set(subreddits)))

    sentiment_type = "positive" if positive else "negative"

    message = (
        f"Most *{sentiment_type}* Reddits in _{subreddits}_ in the last 24 hours:  "
        + "\n\n"
        + "  ".join(message_list)
    )

    if len(message) >= 3000:
        logging.error(
            f"Message is too long (max. 3.000 characters, was: {len(message)})"
        )
        message = message[:2999]

    return message


def send_slack_message(message):
    """
    Function to send a Slack message.
    """

    res = requests.post(
        url=SLACK_WEBHOOK_URL,
        headers={"Content-Type": "application/json"},
        json={
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": message,
                    },
                },
            ],
        },
    )

    print(res.status_code)
    if res.status_code == 200:
        return True

    return False


def set_slacked(post_id):
    """
    Function to set a Reddit as 'slacked' after sending it.
    """

    pg_client_connect = pg_connect()

    query = db.text(
        f"""
        UPDATE {POSTGRES_TABLE}
        SET slacked = 1
        WHERE id = :id;
        """
    )

    pg_client_connect.execute(query, {"id": post_id})
    pg_client_connect.commit()


def slack_one():
    """
    Function to post the Reddit with the highest/lowest sentiment score.
    """

    # Get posts from Postgres
    post = load_last_sentiment_post()

    # Prepare the slack message
    slack_message = prepare_slack_message(post)

    # Send it
    message_sent = send_slack_message(slack_message)

    # Set sent status
    if message_sent:
        set_slacked(post["id"])
        return True

    return False


def slack_list(type="positive"):
    """
    Function to post a list of Reddits.
    """

    positive = True if type == "positive" else False

    # Prepare the slack message
    slack_message = prepare_slack_message_list(positive=positive)

    # If not empty, send it
    if not slack_message == "":
        message_sent = send_slack_message(slack_message)
        if message_sent:
            return True

    return False


def main():
    # Wait for the other jobs to finish
    time.sleep(30)

    slack_one()
    slack_list()


if __name__ == "__main__":
    main()
