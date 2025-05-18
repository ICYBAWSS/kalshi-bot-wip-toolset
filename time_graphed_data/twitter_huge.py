from tweety import Twitter
import sqlite3
import datetime
import os
import re

def main():
    # Get the current time when the script starts
    run_time = datetime.datetime.now().isoformat()

    # Initialize Twitter with your session name 
    app = Twitter("twitter_session")

    # Attempt to connect to an existing Twitter session
    session_file = "twitter_session.tw_session"
    try:
        if os.path.exists(session_file):
            app.connect()
            print(f"Connected to existing session: {app.me}")
        else:
            username = "scrapeeeey76748"
            password = "bottt678$"  # Replace with actual password
            app.sign_in(username, password)
            print(f"Created new session: {app.me}")
    except Exception as e:
        print(f"Error during session handling: {e}")
        return

    # Get user input for category
    category = input("Enter a category (e.g., politics, sports, music): ").strip()
    print(f"\nCategory: {category}")

    # Get user input for primary keywords
    keywords_input = input("\nEnter keywords to search for (separated by commas): ").strip()
    primary_keywords = [k.strip() for k in keywords_input.split(",") if k.strip()]
    if not primary_keywords:
        print("No valid keywords entered. Exiting.")
        return

    # Get supporting keywords for each primary keyword
    supporting_keywords_map = {}
    for pk in primary_keywords:
        supporting_input = input(f"Enter supporting keywords for '{pk}' (comma-separated, e.g., Artificial Intelligence, OpenAI): ").strip()
        supporting_list = [sk.strip() for sk in supporting_input.split(",") if sk.strip()]
        supporting_keywords_map[pk] = [pk.lower()] + [sk.lower() for sk in supporting_list]

    # Get context keywords for category relevance
    context_input = input("\nEnter additional context keywords to ensure tweets are relevant to your category\n"
                          f"(e.g., for '{category}', enter words like 'game,score,season,team' separated by commas): ").strip()
    context_keywords = [ck.strip().lower() for ck in context_input.split(",") if ck.strip()]

    print(f"\nSearching for the following keywords in category '{category}':")
    for idx, pk in enumerate(primary_keywords, 1):
        print(f"{idx}. {pk} (supporting: {', '.join(supporting_keywords_map[pk][1:])})")

    print("\nUsing the following context keywords to filter for relevance:")
    for idx, ck in enumerate(context_keywords, 1):
        print(f"{idx}. {ck}")

    # Database setup
    db_name = 'dataHUGE.db'
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    try:
        cursor.execute('DROP TABLE IF EXISTS tweets')
        conn.commit()
        print("\nDropped existing tweets table to create a fresh dataset.")

        cursor.execute('''
            CREATE TABLE tweets (
                tweet_id TEXT PRIMARY KEY,
                author_id TEXT,
                author_username TEXT,
                author_name TEXT,
                tweet_text TEXT,
                created_at TEXT,
                likes INTEGER,
                retweets INTEGER,
                replies INTEGER,
                views INTEGER,
                search_keyword TEXT,
                category TEXT,
                relevance_score INTEGER,
                collected_at TEXT,
                platform TEXT,
                date_accessed TIMESTAMP
            )
        ''')
        conn.commit()
        print(f"Created a new 'tweets' table in '{db_name}'.")
    except sqlite3.Error as e:
        print(f"Error setting up the database: {e}")
        conn.close()
        return

    now = datetime.datetime.now().isoformat()
    tweets_per_keyword = 100

    for base_keyword in primary_keywords:
        all_search_keywords = supporting_keywords_map[base_keyword]
        fetched_tweets = []
        for search_term in all_search_keywords:
            try:
                print(f"\nFetching tweets for keyword: '{search_term}' (related to '{base_keyword}')")
                results = app.search(search_term)[:tweets_per_keyword]
                print(f"Found {len(results)} tweets for '{search_term}'.")
                fetched_tweets.extend([(tweet, base_keyword) for tweet in results])
            except Exception as e:
                print(f"Error fetching tweets for '{search_term}': {e}")

        print(f"\nProcessing {len(fetched_tweets)} tweets for primary keyword '{base_keyword}'.")
        author_filtered_count = 0
        context_filtered_count = 0
        saved_tweet_count = 0

        for tweet, current_base_keyword in fetched_tweets:
            try:
                author_username_lower = tweet.author.username.lower() if hasattr(tweet, 'author') and hasattr(tweet.author, 'username') else ""
                author_name_lower = tweet.author.name.lower() if hasattr(tweet, 'author') and hasattr(tweet.author, 'name') else ""

                is_author_related = any(sk in author_username_lower or sk in author_name_lower for sk in supporting_keywords_map[current_base_keyword])
                if is_author_related:
                    author_filtered_count += 1
                    continue

                tweet_text_lower = tweet.text.lower() if hasattr(tweet, 'text') else ""
                relevance = sum(1 for ctx in context_keywords if ctx in tweet_text_lower) if context_keywords else 1

                if context_keywords and relevance == 0:
                    context_filtered_count += 1
                    continue

                tweet_data = (
                    str(tweet.id),
                    str(tweet.author.id) if hasattr(tweet, 'author') else None,
                    tweet.author.username if hasattr(tweet, 'author') else None,
                    tweet.author.name if hasattr(tweet, 'author') else None,
                    tweet.text if hasattr(tweet, 'text') else None,
                    tweet.created_on.isoformat() if hasattr(tweet, 'created_on') and tweet.created_on else None,
                    tweet.likes if hasattr(tweet, 'likes') else 0,
                    tweet.retweets if hasattr(tweet, 'retweets') else 0,
                    tweet.replies if hasattr(tweet, 'replies') else 0,
                    tweet.views if hasattr(tweet, 'views') else 0,
                    current_base_keyword,
                    category,
                    relevance,
                    now,  # collected_at timestamp
                    "twitter",
                    run_time  # date_accessed timestamp (time the script was run)
                )
                cursor.execute('''
                    INSERT INTO tweets (tweet_id, author_id, author_username, author_name, tweet_text, created_at,
                    likes, retweets, replies, views, search_keyword, category, relevance_score, collected_at, platform, date_accessed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', tweet_data)
                saved_tweet_count += 1
            except Exception as e:
                print(f"Error processing tweet '{tweet.id}': {e}")

        conn.commit()
        print(f"Saved {saved_tweet_count} tweets for primary keyword '{base_keyword}'.")
        print(f"Filtered out {author_filtered_count} tweets due to author name/username relevance.")
        print(f"Filtered out {context_filtered_count} tweets due to lack of category context.")

    # Summary of collected data
    try:
        cursor.execute("SELECT COUNT(*) FROM tweets")
        total_tweets = cursor.fetchone()[0]
        print(f"\nTotal tweets in the database: {total_tweets}")

        if total_tweets > 0:
            cursor.execute("SELECT search_keyword, COUNT(*), AVG(relevance_score) FROM tweets GROUP BY search_keyword")
            keyword_stats = cursor.fetchall()
            print(f"\nTweets in category '{category}' by primary keyword:")
            for keyword, count, avg_relevance in keyword_stats:
                print(f"  {keyword}: {count} tweets (avg relevance: {avg_relevance:.1f})")

            print("\nExamples of high-relevance tweets:")
            cursor.execute("""
                SELECT search_keyword, tweet_text, relevance_score
                FROM tweets
                ORDER BY relevance_score DESC
                LIMIT 3
            """)
            example_tweets = cursor.fetchall()
            for i, (keyword, text, score) in enumerate(example_tweets, 1):
                short_text = (text[:100] + "...") if len(text) > 100 else text
                print(f"\n{i}. Primary Keyword '{keyword}' (relevance: {score}):\n   {short_text}")

    except sqlite3.Error as e:
        print(f"Error retrieving summary data from the database: {e}")

    finally:
        conn.close()
        print("\nSearch completed and data aggregated to the SQL database.")

if __name__ == "__main__":
    main()