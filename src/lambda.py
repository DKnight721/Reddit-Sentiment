# lambda_function.py
import json
import time
import os
from datetime import datetime, timedelta
from reddit_call import fetch_reddit_data
from clean_data import analyze_sentiment, extract_themes
from db_upload import upload_to_db

# List of subreddits to analyze
SUBREDDITS = ["wallstreetbets", "ecommerce", "suns", "worldnews", "Entrepreneur"]


def process_subreddit(subreddit):
    """Process a single subreddit"""
    print(f"Processing r/{subreddit}...")

    # Fetch data from Reddit
    posts = fetch_reddit_data(subreddit)

    if not posts:
        print(f"No recent posts found for r/{subreddit}")
        return []

    processed_data = []
    all_texts = []

    # Process each post
    for post in posts:
        # Combine title, post content, and comments for sentiment analysis
        title = post.get("title", "")
        selftext = post.get("selftext", "")
        comments = post.get("comments", [])

        post_text = f"{title} {selftext}"
        all_texts.append(post_text)
        all_texts.extend(comments)

        # Analyze sentiment
        sentiment = analyze_sentiment(post_text)

        # Add comment sentiment analysis
        comment_sentiments = [analyze_sentiment(comment) for comment in comments if comment]
        avg_comment_vader = sum(s["vader"] for s in comment_sentiments) / len(
            comment_sentiments) if comment_sentiments else 0

        # Create processed item (only keeping what we need for aggregation)
        processed_item = {
            "subreddit": subreddit,
            "vader_score": sentiment["vader"],
            "roberta_label": sentiment["roberta"],
            "roberta_score": sentiment["roberta_score"],
            "comment_vader_avg": avg_comment_vader,
            "upvote_ratio": post.get("upvote_ratio", 0),
            "score": post.get("score", 0),
            "num_comments": post.get("num_comments", 0)
        }

        processed_data.append(processed_item)

    # Extract common themes across all content
    themes = extract_themes(all_texts)

    # Add themes to each item
    for item in processed_data:
        item["themes"] = themes

    return processed_data


def lambda_handler(event, context):
    """Lambda handler function"""
    start_time = time.time()
    all_results = []
    error_count = 0

    # Process each subreddit
    for subreddit in SUBREDDITS:
        try:
            subreddit_data = process_subreddit(subreddit)
            all_results.extend(subreddit_data)
        except Exception as e:
            print(f"Error processing r/{subreddit}: {e}")
            error_count += 1

    # Upload results to database
    if all_results:
        success = upload_to_db(all_results)
        if not success:
            print("Failed to upload data to database")

    # Calculate statistics
    duration = time.time() - start_time
    post_count = len(all_results)
    subreddit_count = len(SUBREDDITS) - error_count

    response = {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Reddit sentiment analysis completed",
            "stats": {
                "subreddits_processed": subreddit_count,
                "posts_analyzed": post_count,
                "duration_seconds": round(duration, 2),
                "errors": error_count,
                "timestamp": datetime.now().isoformat()
            }
        })
    }

    print(f"Processed {post_count} posts from {subreddit_count} subreddits in {round(duration, 2)} seconds")
    return response


# For local testing
if __name__ == "__main__":
    lambda_handler({}, None)