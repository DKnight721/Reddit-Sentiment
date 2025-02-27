# db_upload.py
import os
import json
from datetime import datetime
from dotenv import load_dotenv
from collections import defaultdict
from supabase import create_client, Client

# Load environment variables
load_dotenv()

# Initialize Supabase client
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")
supabase = create_client(url, key)


def upload_to_supabase(aggregated_data):
    """Upload aggregated sentiment data to Supabase"""
    try:
        for subreddit, data in aggregated_data.items():
            # Convert top_themes to JSON string since Supabase expects it
            data_copy = data.copy()
            data_copy['top_themes'] = json.dumps(data_copy.get('top_themes', []))

            # Insert data using upsert (update if exists)
            result = supabase.table('subreddit').upsert(
                {
                    'subreddit_name': subreddit,
                    'timestamp': data_copy['timestamp'].isoformat(),
                    'vader_avg': data_copy['vader_avg'],
                    'roberta_positive_pct': data_copy['roberta_positive_pct'],
                    'roberta_negative_pct': data_copy['roberta_negative_pct'],
                    'roberta_neutral_pct': data_copy['roberta_neutral_pct'],
                    'post_count': data_copy['post_count'],
                    'comment_count': data_copy['comment_count'],
                    'avg_upvote_ratio': data_copy['avg_upvote_ratio'],
                    'avg_score': data_copy['avg_score'],
                    'top_themes': data_copy['top_themes'],
                    'sentiment_trend': data_copy['sentiment_trend']
                }
            ).execute()

        # Get historical sentiment data for trend calculation (for next run)
        today = datetime.now().date()
        thirty_days_ago = (today - timedelta(days=30)).isoformat()

        result = supabase.table('subreddit') \
            .select('subreddit_name,timestamp,vader_avg') \
            .lt('timestamp', today.isoformat()) \
            .gte('timestamp', thirty_days_ago) \
            .order('timestamp', desc=True) \
            .execute()

        historical_data = result.data if hasattr(result, 'data') else []

        return True
    except Exception as e:
        print(f"Error uploading to Supabase: {e}")
        return False


def aggregate_sentiment_data(processed_data):
    """Aggregate individual post data into daily subreddit summaries"""
    aggregated = defaultdict(lambda: {
        'vader_scores': [],
        'roberta_labels': [],
        'roberta_scores': [],
        'upvote_ratios': [],
        'scores': [],
        'post_count': 0,
        'comment_count': 0,
        'all_themes': []
    })

    # Group and aggregate data by subreddit
    for item in processed_data:
        subreddit = item.get('subreddit')

        # Skip items with missing key data
        if not subreddit:
            continue

        # Add sentiment scores
        aggregated[subreddit]['vader_scores'].append(item.get('vader_score', 0))
        aggregated[subreddit]['roberta_labels'].append(item.get('roberta_label', 'NEUTRAL'))
        aggregated[subreddit]['roberta_scores'].append(item.get('roberta_score', 0.5))

        # Add engagement metrics
        aggregated[subreddit]['upvote_ratios'].append(item.get('upvote_ratio', 0))
        aggregated[subreddit]['scores'].append(item.get('score', 0))

        # Count posts and comments
        aggregated[subreddit]['post_count'] += 1
        aggregated[subreddit]['comment_count'] += item.get('num_comments', 0)

        # Add themes
        if 'themes' in item and item['themes']:
            aggregated[subreddit]['all_themes'].extend(item['themes'])

    # Calculate final aggregated values
    result = {}
    today = datetime.now().date()

    for subreddit, data in aggregated.items():
        # Skip subreddits with no data
        if data['post_count'] == 0:
            continue

        # Calculate averages
        vader_avg = sum(data['vader_scores']) / len(data['vader_scores']) if data['vader_scores'] else 0

        # Calculate Roberta distribution
        roberta_counts = {'POSITIVE': 0, 'NEGATIVE': 0, 'NEUTRAL': 0}
        for label in data['roberta_labels']:
            if label in roberta_counts:
                roberta_counts[label] += 1

        total_labels = len(data['roberta_labels'])
        roberta_positive_pct = (roberta_counts['POSITIVE'] / total_labels * 100) if total_labels > 0 else 0
        roberta_negative_pct = (roberta_counts['NEGATIVE'] / total_labels * 100) if total_labels > 0 else 0
        roberta_neutral_pct = (roberta_counts['NEUTRAL'] / total_labels * 100) if total_labels > 0 else 0

        # Calculate engagement averages
        avg_upvote_ratio = sum(data['upvote_ratios']) / len(data['upvote_ratios']) if data['upvote_ratios'] else 0
        avg_score = sum(data['scores']) / len(data['scores']) if data['scores'] else 0

        # Aggregate themes
        theme_counts = defaultdict(int)
        for theme, count in data['all_themes']:
            theme_counts[theme] += count

        top_themes = sorted(theme_counts.items(), key=lambda x: x[1], reverse=True)[:10]

        # We'll set sentiment_trend to 0 initially
        # After we have historical data, this will be calculated by the lambda function
        result[subreddit] = {
            'timestamp': today,
            'vader_avg': vader_avg,
            'roberta_positive_pct': roberta_positive_pct,
            'roberta_negative_pct': roberta_negative_pct,
            'roberta_neutral_pct': roberta_neutral_pct,
            'post_count': data['post_count'],
            'comment_count': data['comment_count'],
            'avg_upvote_ratio': avg_upvote_ratio,
            'avg_score': avg_score,
            'top_themes': top_themes,
            'sentiment_trend': 0.0  # Will be updated with historical comparison
        }

    return result


def upload_to_db(processed_data):
    """Process individual post data into aggregated metrics and store in database"""
    # Aggregate the data
    aggregated_data = aggregate_sentiment_data(processed_data)

    # Upload to Supabase
    success = upload_to_supabase(aggregated_data)

    return bool(aggregated_data) and success