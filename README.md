# Reddit Sentiment Analysis

### Implementation of an ETL pipeline that collects Reddit posts, applies a sentiment analysis, and publishes selected posts on Slack in real time, passing two steps of database storage (MongoDB and PostgreSQL)

In this project, I built an ETL pipeline with multiple steps:

1. **Collect posts from Reddit:** A script gets Reddits from the Reddit API and inserts them to a MongoDB. (see directory `reddit_collector`)
2. **Transform Reddit posts:** An ETL job extracts data from MongoDB, transforms it including Sentiment Analysis, and loads it into a PostgreSQL database. (see directory `etl_job`)
3. **Publish selected posts in Slack:** In the last step, data on the posts including results of the Sentiment Analysis are loaded and sent as Slack messages. (see directory `slack_bot`)

### Run the process

The whole pipeline runs using **Docker** and **Docker Compose**. To run it you need to:

1. Have Docker and Docker Compose installed
2. Have a Reddit account and create a "secret" for the API.
3. Create a Slackbot Webhook URL to be able to send Slack messages.
4. Define the following configuration in a file called `.env` in the same directory as `docker-compose.yml`:

```
# Credentials to be used when PostgreSQL database is created
POSTGRES_USER=""
POSTGRES_PASSWORD=""
POSTGRES_DB=""
POSTGRES_TABLE=""

# Webhook URL to send Slack messages
SLACK_WEBHOOK_URL=""

# Reddit credentials
REDDIT_CLIENT_ID=""
REDDIT_SECRET=""
REDDIT_USER=""
REDDIT_PWD=""
```

Then you clone the repository, build the Docker images using Docker Compose, and run it:

```bash
git clone https://github.com/yotkadata/reddit_etl.git
cd reddit_etl/
docker-compose build
docker-compose up -d
```

Skip the `-d` parameter in `docker-compose run` to se output in terminal and not run it in the background.

To see the content of the MongoDB or the PostgreSQL database, you can attach to one of the running containers. First, get the name of the process by showing all running containers:

```bash
docker-compose ps
```

Then you can access the MongoDB shell this way and, for example, show some content in the database:

```
docker exec -it <CONTAINER_NAME> mongosh

# In Mongo shell, show databases
show dbs

# Select a database to use (in this case: reddit_posts)
use reddit_posts

# Show existing tables (collections)
show collections

# Count number of documents in collection (in this case: posts)
db.posts.countDocuments()

# Show content of collection "posts
db.posts.find().pretty()
```

Similarly, you can access the PostgreSQL database and inspect its content in the psql shell:

```
docker exec -it <CONTAINER_NAME> psql -p 5432 -U postgres

# Show databases
\l

# Connect to database called "reddit"
\c reddit

# Show tables
\dt

# Show some columns of saved posts
SELECT id, date, sub_id, subreddit, author_id, author, LEFT(title, 50), upvote_ratio, num_comments, sentiment, slacked FROM posts LIMIT 10;
```

To stop and remove running containers, run

```
docker-compose down

# Show images
docker image

# Remove image by name
docker image rm <IMAGE_NAME>
```
