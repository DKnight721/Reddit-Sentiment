import praw
import os
from dotenv import load_dotenv
import json


# Load .env file
load_dotenv()


# Get environment variables (ensure variable names match what's in your .env file)
client_id = os.environ.get("client_id")  # Make sure it's lowercase in .env
client_secret = os.environ.get("client_secret")
user_agent = os.environ.get("user_agent")

# Initialize Reddit API
reddit = praw.Reddit(
    client_id=client_id,
    client_secret=client_secret,
    user_agent=user_agent
)

# Define the subreddit and get the top 10 hot posts
subreddit = reddit.subreddit('wallstreetbets')
hot_posts = subreddit.hot(limit=10)

# Create a list to hold all the data
data = []

# Loop through each post to get the top 20 comments
for post in hot_posts:
    post.comments.replace_more(limit=0)  # Remove "More Comments" if it exists
    top_comments = post.comments[:20]  # Get the top 20 comments

    post_data = {
        'post_title': post.title,
        'post_url': post.url,
        'comments': []
    }

    for comment in top_comments:
        post_data['comments'].append({
            'comment_id': comment.id,
            'comment_body': comment.body
        })

    data.append(post_data)

# Save the data to a JSON file
with open('wallstreetbets_data.json', 'w', encoding='utf-8') as file:
    json.dump(data, file, indent=4)

print("Data saved to wallstreetbets_data.json")