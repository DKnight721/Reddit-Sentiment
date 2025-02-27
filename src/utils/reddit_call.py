# reddit_call.py
import praw
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

# Initialize Reddit API client
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT")
)


def fetch_reddit_data(subreddit, time_filter="day", limit=25):
    """
    Fetch top posts from a specified subreddit within the last 24 hours

    Args:
        subreddit (str): Name of the subreddit
        time_filter (str): Time filter for posts ('day', 'week', 'month', 'year', 'all')
        limit (int): Maximum number of posts to fetch

    Returns:
        list: List of dictionaries containing post data
    """
    subreddit_obj = reddit.subreddit(subreddit)
    posts = subreddit_obj.top(time_filter=time_filter, limit=limit)

    data = []
    for post in posts:
        # Calculate post age
        post_time = datetime.fromtimestamp(post.created_utc)
        now = datetime.now()

        # Only include posts from the last 24 hours
        if now - post_time <= timedelta(hours=24):
            # Only fetch a limited number of comments to keep processing lighter
            post.comments.replace_more(limit=0)
            comments = [comment.body for comment in post.comments.list()[:10]]

            data.append({
                "title": post.title,
                "selftext": post.selftext,
                "upvote_ratio": post.upvote_ratio,
                "score": post.score,
                "num_comments": post.num_comments,
                "comments": comments
            })

    print(f"Fetched {len(data)} posts from r/{subreddit}")
    return data