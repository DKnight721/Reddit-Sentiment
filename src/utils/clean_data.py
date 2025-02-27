# clean_data.py
from transformers import pipeline
import re
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from collections import Counter

# Download necessary NLTK packages
nltk.download('vader_lexicon', quiet=True)
nltk.download('punkt', quiet=True)
nltk.download('stopwords', quiet=True)

# Initialize sentiment analyzers
sia = SentimentIntensityAnalyzer()
sentiment_pipeline = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment")


def clean_text(text):
    """Clean and normalize text data"""
    if not text:
        return ""

    # Convert to string if necessary
    if not isinstance(text, str):
        text = str(text)

    # Remove URLs
    text = re.sub(r'http\S+', '', text)
    # Remove special characters but keep spaces and sentence structure
    text = re.sub(r'[^\w\s\.\,\!\?\']', '', text)
    # Replace multiple spaces with a single space
    text = re.sub(r'\s+', ' ', text).strip()

    return text


def analyze_sentiment(text):
    """Analyze sentiment using VADER and RoBERTa models"""
    if not text:
        return {"vader": 0, "roberta": "NEUTRAL", "roberta_score": 0.5}

    cleaned_text = clean_text(text)

    # VADER sentiment analysis
    vader_scores = sia.polarity_scores(cleaned_text)
    vader_compound = vader_scores["compound"]

    # Truncate text if too long for RoBERTa (model limit ~512 tokens)
    if len(cleaned_text) > 1000:
        cleaned_text = cleaned_text[:1000]

    # RoBERTa sentiment analysis
    try:
        roberta_result = sentiment_pipeline(cleaned_text)[0]
        roberta_label = roberta_result["label"]
        roberta_score = roberta_result["score"]
    except Exception as e:
        print(f"Error in RoBERTa analysis: {e}")
        roberta_label = "NEUTRAL"
        roberta_score = 0.5

    return {
        "vader": vader_compound,
        "roberta": roberta_label,
        "roberta_score": roberta_score
    }


def extract_themes(texts, top_n=10):
    """Extract common themes/topics from a collection of texts"""
    if not texts:
        return []

    combined_text = " ".join([clean_text(text) for text in texts if text])

    # Tokenize and remove stopwords
    tokens = word_tokenize(combined_text.lower())
    stop_words = set(stopwords.words('english'))

    # Add additional finance/reddit-specific stopwords
    additional_stopwords = {
        'like', 'just', 'get', 'going', 'make', 'will',
        'know', 'think', 'post', 'reddit', 'edit', 'comment',
        'deleted', 'removed', 'thank', 'thanks', 'good', 'really',
        'time', 'want', 'need', 'well', 'would', 'could', 'should'
    }
    stop_words.update(additional_stopwords)

    filtered_tokens = [word for word in tokens if word.isalpha() and word not in stop_words and len(word) > 3]

    # Count word frequencies
    word_counts = Counter(filtered_tokens)

    # Return top N themes
    return word_counts.most_common(top_n)