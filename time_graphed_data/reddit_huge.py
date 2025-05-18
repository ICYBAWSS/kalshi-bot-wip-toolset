from praw import Reddit
import sqlite3
import datetime
import os
import re

def main():
    # Get the current time when the script starts
    run_time = datetime.datetime.now().isoformat()

    # Initialize Reddit API with your credentials
    try:
        # Replace these with your actual Reddit API credentials
        reddit = Reddit(
            client_id="cJUdb9WBML3lB_LovOhlZQ",
            client_secret="75UNIiXy5gh7LhcKWJ-pkLDxpMICDA",
            user_agent="post_scraper/v1.0 by ivybawss",
            username="hiii7899",
            password="Scrapey34"
        )
        print(f"Connected to Reddit as: {reddit.user.me()}")
    except Exception as e:
        print(f"Error connecting to Reddit: {e}")
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
    context_input = input("\nEnter additional context keywords to ensure posts are relevant to your category\n"
                          f"(e.g., for '{category}', enter words like 'game,score,season,team' separated by commas): ").strip()
    context_keywords = [ck.strip().lower() for ck in context_input.split(",") if ck.strip()]

    print(f"\nSearching for the following keywords in category '{category}':")
    for idx, pk in enumerate(primary_keywords, 1):
        print(f"{idx}. {pk} (supporting: {', '.join(supporting_keywords_map[pk][1:])})")

    print("\nUsing the following context keywords to filter for relevance:")
    for idx, ck in enumerate(context_keywords, 1):
        print(f"{idx}. {ck}")

    # Ask user for subreddits to search
    subreddits_input = input("\nEnter subreddits to search (separated by commas, leave blank for all): ").strip()
    subreddits = [s.strip() for s in subreddits_input.split(",") if s.strip()]
    
    # Ask user for post limit
    try:
        posts_per_keyword = int(input("\nEnter maximum number of posts to fetch per keyword (default 100): ") or "100")
    except ValueError:
        posts_per_keyword = 100
        print("Invalid input. Using default value of 100 posts per keyword.")

    # Database setup
    db_name = 'redditData.db'
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    try:
        cursor.execute('DROP TABLE IF EXISTS reddit_posts')
        conn.commit()
        print("\nDropped existing reddit_posts table to create a fresh dataset.")

        cursor.execute('''
            CREATE TABLE reddit_posts (
                post_id TEXT PRIMARY KEY,
                author_id TEXT,
                author_username TEXT,
                subreddit TEXT,
                post_title TEXT,
                post_text TEXT,
                created_at TEXT,
                score INTEGER,
                upvote_ratio REAL,
                num_comments INTEGER,
                search_keyword TEXT,
                category TEXT,
                relevance_score INTEGER,
                collected_at TEXT,
                platform TEXT,
                date_accessed TIMESTAMP
            )
        ''')
        conn.commit()
        print(f"Created a new 'reddit_posts' table in '{db_name}'.")
    except sqlite3.Error as e:
        print(f"Error setting up the database: {e}")
        conn.close()
        return

    now = datetime.datetime.now().isoformat()

    for base_keyword in primary_keywords:
        all_search_keywords = supporting_keywords_map[base_keyword]
        fetched_posts = []
        
        for search_term in all_search_keywords:
            try:
                print(f"\nFetching posts for keyword: '{search_term}' (related to '{base_keyword}')")
                
                # Search in specific subreddits if provided, otherwise search all of Reddit
                if subreddits:
                    for sub in subreddits:
                        subreddit = reddit.subreddit(sub)
                        results = list(subreddit.search(search_term, limit=posts_per_keyword))
                        print(f"Found {len(results)} posts for '{search_term}' in r/{sub}")
                        fetched_posts.extend([(post, base_keyword, sub) for post in results])
                else:
                    results = list(reddit.subreddit("all").search(search_term, limit=posts_per_keyword))
                    print(f"Found {len(results)} posts for '{search_term}' across all subreddits")
                    fetched_posts.extend([(post, base_keyword, post.subreddit.display_name) for post in results])
                    
            except Exception as e:
                print(f"Error fetching posts for '{search_term}': {e}")

        print(f"\nProcessing {len(fetched_posts)} posts for primary keyword '{base_keyword}'.")
        author_filtered_count = 0
        context_filtered_count = 0
        saved_post_count = 0

        for post, current_base_keyword, subreddit_name in fetched_posts:
            try:
                # Skip deleted/removed authors
                if post.author is None:
                    continue
                    
                author_username_lower = post.author.name.lower()

                # Filter out posts from authors whose names contain keywords (likely bots or topic-specific accounts)
                is_author_related = any(sk in author_username_lower for sk in supporting_keywords_map[current_base_keyword])
                if is_author_related:
                    author_filtered_count += 1
                    continue

                # Combine title and selftext for analysis
                post_title_lower = post.title.lower() if hasattr(post, 'title') else ""
                post_text_lower = post.selftext.lower() if hasattr(post, 'selftext') else ""
                combined_text = post_title_lower + " " + post_text_lower
                
                # Calculate relevance score based on context keywords
                relevance = sum(1 for ctx in context_keywords if ctx in combined_text) if context_keywords else 1

                if context_keywords and relevance == 0:
                    context_filtered_count += 1
                    continue

                created_time = datetime.datetime.fromtimestamp(post.created_utc).isoformat() if hasattr(post, 'created_utc') else None
                
                post_data = (
                    post.id,
                    post.author.id if hasattr(post.author, 'id') else None,
                    post.author.name,
                    subreddit_name,
                    post.title if hasattr(post, 'title') else None,
                    post.selftext if hasattr(post, 'selftext') else None,
                    created_time,
                    post.score if hasattr(post, 'score') else 0,
                    post.upvote_ratio if hasattr(post, 'upvote_ratio') else 0,
                    post.num_comments if hasattr(post, 'num_comments') else 0,
                    current_base_keyword,
                    category,
                    relevance,
                    now,  # collected_at timestamp
                    "reddit",
                    run_time  # date_accessed timestamp (time the script was run)
                )
                
                cursor.execute('''
                    INSERT INTO reddit_posts (post_id, author_id, author_username, subreddit, post_title, post_text, 
                    created_at, score, upvote_ratio, num_comments, search_keyword, category, relevance_score, 
                    collected_at, platform, date_accessed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', post_data)
                saved_post_count += 1
            except Exception as e:
                print(f"Error processing post '{post.id}': {e}")

        conn.commit()
        print(f"Saved {saved_post_count} posts for primary keyword '{base_keyword}'.")
        print(f"Filtered out {author_filtered_count} posts due to author username relevance.")
        print(f"Filtered out {context_filtered_count} posts due to lack of category context.")

    # Summary of collected data
    try:
        cursor.execute("SELECT COUNT(*) FROM reddit_posts")
        total_posts = cursor.fetchone()[0]
        print(f"\nTotal posts in the database: {total_posts}")

        if total_posts > 0:
            cursor.execute("SELECT search_keyword, COUNT(*), AVG(relevance_score) FROM reddit_posts GROUP BY search_keyword")
            keyword_stats = cursor.fetchall()
            print(f"\nPosts in category '{category}' by primary keyword:")
            for keyword, count, avg_relevance in keyword_stats:
                print(f"  {keyword}: {count} posts (avg relevance: {avg_relevance:.1f})")

            cursor.execute("SELECT subreddit, COUNT(*) FROM reddit_posts GROUP BY subreddit ORDER BY COUNT(*) DESC LIMIT 5")
            subreddit_stats = cursor.fetchall()
            print("\nTop 5 subreddits in the dataset:")
            for subreddit, count in subreddit_stats:
                print(f"  r/{subreddit}: {count} posts")

            print("\nExamples of high-relevance posts:")
            cursor.execute("""
                SELECT search_keyword, post_title, relevance_score, subreddit
                FROM reddit_posts
                ORDER BY relevance_score DESC
                LIMIT 3
            """)
            example_posts = cursor.fetchall()
            for i, (keyword, title, score, subreddit) in enumerate(example_posts, 1):
                short_title = (title[:100] + "...") if len(title) > 100 else title
                print(f"\n{i}. Primary Keyword '{keyword}' (relevance: {score}, subreddit: r/{subreddit}):\n   {short_title}")

    except sqlite3.Error as e:
        print(f"Error retrieving summary data from the database: {e}")

    finally:
        conn.close()
        print("\nSearch completed and data aggregated to the SQL database.")

if __name__ == "__main__":
    main()