import sqlite3
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import numpy as np
from scipy.special import softmax
from collections import defaultdict
import spacy
import re
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load RoBERTa model and tokenizer for sentiment analysis
MODEL = "cardiffnlp/twitter-roberta-base-sentiment"
tokenizer = AutoTokenizer.from_pretrained(MODEL)
model = AutoModelForSequenceClassification.from_pretrained(MODEL)

# Load SpaCy for NER
nlp = spacy.load("en_core_web_sm")

# Label mapping from model
label_map = {0: "negative", 1: "neutral", 2: "positive"}

# NEW: Empathy indicators and their reduction factors
EMPATHY_INDICATORS = {
    "poor": 0.6,       # Reduce negative impact by 60%
    "hope": 0.4,       # Reduce negative impact by 60%
    "prayers": 0.3,    # Reduce negative impact by 70%
    "get well": 0.2,   # Reduce negative impact by 80%
    "wishing": 0.3,    # Reduce negative impact by 70%
    "sorry": 0.5,      # Reduce negative impact by 50%
    "sad to": 0.4,     # Reduce negative impact by 60%
    "unfortunate": 0.5 # Reduce negative impact by 50%
}

# NEW: State description terms that might indicate empathy context
STATE_TERMS = [
    "looks", "seems", "appears", "is", "was", "feeling",
    "sad", "depressed", "upset", "down", "ill", "sick",
    "struggling", "hurting", "suffering", "tired"
]

def preprocess(text):
    """Enhanced preprocessing for social media text."""
    if text is None:
        return ""
    text = str(text)
    # Remove URLs, hashtags, mentions, and excessive whitespace
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    text = re.sub(r'#\w+|\@\w+', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def extract_entity_context(text, entity):
    """Extract context around the entity for sentiment analysis."""
    doc = nlp(text.lower())
    entity_sentences = []
    for sent in doc.sents:
        # Check for exact entity match or close proximity
        if entity.lower() in sent.text.lower() or any(ent.text.lower() == entity.lower() for ent in sent.ents):
            entity_sentences.append(sent.text)
    return " ".join(entity_sentences) if entity_sentences else text

# NEW: Function to classify sentiment context
def classify_sentiment_context(text, entity):
    """Determine if sentiment is about or attributed to the entity."""
    text_lower = text.lower()
    entity_lower = entity.lower()
    
    # Pattern 1: Empathy expressions (poor entity, sad about entity)
    empathy_pattern1 = rf"(?:poor|oh no|sad|tragic|unfortunate).*{re.escape(entity_lower)}"
    
    # Pattern 2: State descriptions (entity looks/seems sad/depressed)
    state_terms_re = "|".join(STATE_TERMS)
    empathy_pattern2 = rf"{re.escape(entity_lower)}.*(?:{state_terms_re})"
    
    if re.search(empathy_pattern1, text_lower) or re.search(empathy_pattern2, text_lower):
        return "attributed"  # Sentiment describes entity's state
    
    return "direct"  # Sentiment is directly about entity

# NEW: Function to adjust sentiment based on empathy detection
def adjust_for_empathy(text, entity, sentiment_score):
    """Adjust sentiment when empathy is detected."""
    if sentiment_score >= 0:
        return sentiment_score  # Only adjust negative scores
        
    context_type = classify_sentiment_context(text, entity)
    
    if context_type == "attributed":
        logger.debug(f"Detected attributed sentiment context for '{entity}' in: '{text}'")
        
        # For empathetic content, reduce negative weight
        adjusted_score = sentiment_score * 0.3  # Dampen negative score
        
        # Option for potentially flipping very negative scores in clear empathy cases
        if sentiment_score < -0.6 and any(term in text.lower() for term in ['poor', 'prayers', 'hope']):
            # Consider strong empathy as slightly positive sentiment toward the person
            adjusted_score = -sentiment_score * 0.2  # Flip and heavily dampen
            
        logger.debug(f"Adjusted score from {sentiment_score:.4f} to {adjusted_score:.4f} due to empathy context")
        return adjusted_score
    
    return sentiment_score

# NEW: Function to adjust sentiment based on empathy keywords
def adjust_for_empathy_keywords(text, sentiment_score):
    """Adjust sentiment based on empathy-indicating terms."""
    if sentiment_score >= 0:
        return sentiment_score  # Only adjust negative scores
    
    text_lower = text.lower()
    original_score = sentiment_score
    
    for term, reduction_factor in EMPATHY_INDICATORS.items():
        if term in text_lower:
            adjusted_score = sentiment_score * reduction_factor
            logger.debug(f"Reduced negative sentiment impact due to '{term}': {sentiment_score:.4f} -> {adjusted_score:.4f}")
            return adjusted_score
            
    return sentiment_score

def adjust_sentiment_for_context(text, sentiment_score, entity):
    """Adjust sentiment score based on empathy context."""
    # NEW: Add empathy detection and adjustment
    sentiment_score = adjust_for_empathy(text, entity, sentiment_score)
    sentiment_score = adjust_for_empathy_keywords(text, sentiment_score)
    
    return sentiment_score

def add_columns_if_missing(cursor, table_name):
    """Add missing columns to the database table."""
    try:
        cursor.execute(f"PRAGMA table_info({table_name});")
        existing_columns = [col[1] for col in cursor.fetchall()]
        for column, col_type in [("sentiment_score_flair", "REAL"), ("sentiment_category_flair", "TEXT")]:
            if column not in existing_columns:
                logger.info(f"Adding column {column} to {table_name}")
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} {col_type};")
    except sqlite3.OperationalError as e:
        logger.error(f"Error adding columns to {table_name}: {e}")

def check_table_structure(conn):
    """Check and log database table structure."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    logger.info(f"Tables in database: {[t[0] for t in tables]}")
    for table in tables:
        table_name = table[0]
        logger.info(f"Structure of table '{table_name}':")
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        for col in columns:
            logger.info(f"  {col[1]} ({col[2]})")

def analyze_sentiment_roberta_recalibrated():
    """Perform sentiment analysis on tweets and TikToks."""
    conn = sqlite3.connect('dataHUGE.db')
    cursor = conn.cursor()
    
    logger.info("Checking database structure...")
    check_table_structure(conn)
    
    add_columns_if_missing(cursor, 'tweets')
    add_columns_if_missing(cursor, 'tiktoks')
    
    cursor.execute("SELECT COUNT(*) FROM tweets")
    tweet_count = cursor.fetchone()[0]
    logger.info(f"Number of tweets in database: {tweet_count}")
    
    cursor.execute("SELECT COUNT(*) FROM tiktoks")
    tiktok_count = cursor.fetchone()[0]
    logger.info(f"Number of tiktoks in database: {tiktok_count}")

    cursor.execute("SELECT tweet_id, tweet_text, search_keyword, likes FROM tweets")
    twitter_tweets = cursor.fetchall()
    logger.info(f"Fetched {len(twitter_tweets)} tweets for analysis")

    cursor.execute("SELECT video_id, description, search_keyword, likes FROM tiktoks")
    tiktoks_videos = cursor.fetchall()
    logger.info(f"Fetched {len(tiktoks_videos)} tiktoks for analysis")

    all_data = twitter_tweets + tiktoks_videos
    max_likes = max([item[3] for item in all_data]) if all_data else 0
    logger.info(f"Max likes across all data: {max_likes}")

    sentiment_distribution = {"positive": 0, "neutral": 0, "negative": 0}
    highest_sentiment = {'score': -float('inf'), 'text': '', 'weighted_score': -float('inf')}
    lowest_sentiment = {'score': float('inf'), 'text': '', 'weighted_score': float('inf')}

    keyword_sentiment = defaultdict(list)
    keyword_likes = defaultdict(list)
    keyword_count = defaultdict(int)
    empathy_adjusted_count = 0  # Track how many items were adjusted for empathy

    sample_data = []
    update_count = {'tweets': 0, 'tiktoks': 0}

    for item in all_data:
        id_val, text, keywords, likes = item
        
        text = preprocess(text)
        if not text:
            logger.warning(f"Skipping item with empty text (ID: {id_val})")
            continue

        logger.debug(f"Processing item: ID={id_val}, Text='{text[:30]}...', Keywords='{keywords}', Likes={likes}")
            
        try:
            # Extract context and compute sentiment for each keyword
            sentiment_scores = []
            for keyword in [k.strip() for k in keywords.split(',') if k.strip()]:
                context_text = extract_entity_context(text, keyword)
                inputs = tokenizer(context_text, return_tensors="pt", truncation=True, max_length=512)
                with torch.no_grad():
                    outputs = model(**inputs)
                scores = softmax(outputs.logits[0].numpy())
                
                raw_sentiment_score = scores[2] - scores[0]  # positive - negative
                
                # Store original score for logging
                original_score = raw_sentiment_score
                
                # NEW: Check if this is an empathy context before other adjustments
                context_type = classify_sentiment_context(context_text, keyword)
                
                # Adjust for empathy context
                sentiment_score = adjust_sentiment_for_context(context_text, raw_sentiment_score, keyword)
                
                # Track empathy adjustments
                if abs(original_score - sentiment_score) > 0.1:
                    empathy_adjusted_count += 1
                    logger.info(f"Empathy adjustment applied: '{context_text[:50]}...' - Score changed from {original_score:.4f} to {sentiment_score:.4f}")
                
                # Filter extreme outliers
                if abs(sentiment_score) > 1.5:  # Arbitrary threshold
                    logger.warning(f"Skipping outlier score for {keyword}: {sentiment_score:.4f}")
                    continue
                
                # Cap likes weighting
                capped_likes = min(likes, 1000)  # Prevent viral posts from dominating
                weighted_score = sentiment_score * (capped_likes / max_likes) if max_likes > 0 else sentiment_score

                sentiment_scores.append((sentiment_score, weighted_score, context_text, original_score))

            if not sentiment_scores:
                continue

            # Average sentiment scores for the item
            sentiment_score = np.mean([s[0] for s in sentiment_scores])
            weighted_score = np.mean([s[1] for s in sentiment_scores])
            context_text = sentiment_scores[0][2]  # Use first context for logging
            original_score = sentiment_scores[0][3]  # Use first original score for logging

            sentiment_category = (
                "positive" if weighted_score > 0.1 else
                "neutral" if weighted_score >= -0.1 else
                "negative"
            )

            logger.debug(f"Calculated sentiment: Original={original_score:.4f}, Adjusted={sentiment_score:.4f}, Weighted={weighted_score:.4f}, Category={sentiment_category}")
            
            if weighted_score > highest_sentiment['weighted_score']:
                highest_sentiment.update({'score': sentiment_score, 'text': text, 'weighted_score': weighted_score})
            if weighted_score < lowest_sentiment['weighted_score']:
                lowest_sentiment.update({'score': sentiment_score, 'text': text, 'weighted_score': weighted_score})

            sentiment_distribution[sentiment_category] += 1

            if len(sample_data) < 5:
                sample_data.append((text, scores, original_score, sentiment_score, weighted_score))

            is_twitter = id_val in [t[0] for t in twitter_tweets]
            
            if is_twitter:
                logger.debug(f"Updating tweet (ID: {id_val}) with sentiment {weighted_score:.4f}, {sentiment_category}")
                cursor.execute('''
                    UPDATE tweets
                    SET sentiment_score_flair = ?, sentiment_category_flair = ?
                    WHERE tweet_id = ?
                ''', (weighted_score, sentiment_category, id_val))
                update_count['tweets'] += 1
            else:
                logger.debug(f"Updating tiktok (ID: {id_val}) with sentiment {weighted_score:.4f}, {sentiment_category}")
                cursor.execute('''
                    UPDATE tiktoks
                    SET sentiment_score_flair = ?, sentiment_category_flair = ?
                    WHERE video_id = ?
                ''', (weighted_score, sentiment_category, id_val))
                update_count['tiktoks'] += 1

            conn.commit()
            
            if keywords:
                for keyword in [k.strip() for k in keywords.split(',')]:
                    if keyword:
                        keyword_sentiment[keyword].append(sentiment_score)
                        keyword_likes[keyword].append(capped_likes)
                        keyword_count[keyword] += 1
        except Exception as e:
            logger.error(f"Error processing item with ID {id_val}: {str(e)}")

    logger.info("Verifying updates...")
    cursor.execute("SELECT COUNT(*) FROM tweets WHERE sentiment_score_flair IS NOT NULL")
    updated_tweets = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM tiktoks WHERE sentiment_score_flair IS NOT NULL")
    updated_tiktoks = cursor.fetchone()[0]
    
    logger.info(f"Updated tweets: {updated_tweets}/{tweet_count} (Expected: {update_count['tweets']})")
    logger.info(f"Updated tiktoks: {updated_tiktoks}/{tiktok_count} (Expected: {update_count['tiktoks']})")
    logger.info(f"Empathy adjustments applied: {empathy_adjusted_count} items")

    avg_sentiment_by_keyword = {}
    for keyword in keyword_sentiment:
        total_likes = sum(keyword_likes[keyword])
        if total_likes > 0:
            weighted_sum = sum(score * (likes / total_likes)
                               for score, likes in zip(keyword_sentiment[keyword], keyword_likes[keyword]))
            avg_sentiment_by_keyword[keyword] = weighted_sum
        else:
            avg_sentiment_by_keyword[keyword] = sum(keyword_sentiment[keyword]) / len(keyword_sentiment[keyword]) if keyword_sentiment[keyword] else 0

    sorted_keywords = sorted(
        avg_sentiment_by_keyword.items(), 
        key=lambda x: x[1], 
        reverse=True
    )

    print("\n--- Sentiment Distribution ---")
    total = sum(sentiment_distribution.values())
    for cat, count in sentiment_distribution.items():
        percent = (count / total) * 100 if total > 0 else 0
        print(f"{cat}: {count} items ({percent:.1f}%)")

    print("\n--- Sample Data Analysis ---")
    for i, (text, scores, original, adjusted, weighted) in enumerate(sample_data):
        print(f"Item {i+1}: \"{text[:50]}...\"")
        print(f"  Raw scores: {scores}")
        print(f"  Original: {original:.4f}, Adjusted: {adjusted:.4f}, Weighted: {weighted:.4f}")
        if abs(original - adjusted) > 0.1:
            print(f"  Note: Empathy adjustment applied (difference: {adjusted-original:.4f})")

    print("\n--- Sentiment Analysis Results (with Empathy Detection) ---")
    print(f"\nTotal items with empathy-based adjustments: {empathy_adjusted_count}")
    print("\nLikes-weighted average sentiment scores for each keyword (SORTED HIGH TO LOW):")
    print("\n{:<30} {:<10} {:<15} {:<10}".format("Keyword", "Score", "Signal", "Post Count"))
    print("-" * 65)
    
    for keyword, avg_sentiment in sorted_keywords:
        bet_signal = "Bet YES" if avg_sentiment > 0.2 else "Bet NO" if avg_sentiment < -0.2 else "Hold"
        print("{:<30} {:<10.4f} {:<15} {:<10}".format(
            keyword, 
            avg_sentiment, 
            bet_signal,
            keyword_count[keyword]
        ))

    conn.commit()
    conn.close()

    print("\nSentiment analysis completed and updated in the database.")
    print(f"\nItem with HIGHEST sentiment:\nScore: {highest_sentiment['score']:.4f} (Weighted: {highest_sentiment['weighted_score']:.4f})\nText: \"{highest_sentiment['text']}\"")
    print(f"\nItem with LOWEST sentiment:\nScore: {lowest_sentiment['score']:.4f} (Weighted: {lowest_sentiment['weighted_score']:.4f})\nText: \"{lowest_sentiment['text']}\"")

if __name__ == "__main__":
    analyze_sentiment_roberta_recalibrated()