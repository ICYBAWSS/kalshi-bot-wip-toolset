from TikTokApi import TikTokApi
import sqlite3
import datetime
import asyncio

async def fetch_tiktoks_for_keyword(api, keyword, max_tiktoks):
    """Fetch TikToks for a single keyword."""
    all_tiktoks = []
    try:
        print(f"\nFetching TikToks for keyword: {keyword}")
        hashtag = api.hashtag(name=keyword)
        async for tiktok in hashtag.videos(count=max_tiktoks):
            all_tiktoks.append((tiktok.as_dict, keyword))
        print(f"Fetched {len(all_tiktoks[-max_tiktoks:])} TikToks for '{keyword}'")
    except Exception as e:
        print(f"Error fetching TikToks for keyword '{keyword}': {str(e)}")
    return all_tiktoks

def create_database():
    """Create a fresh SQLite database and return the cursor."""
    conn = sqlite3.connect('dataHUGE.db')
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS tiktoks')
    conn.commit()
    print("\nDropped existing TikToks table to create fresh dataset")

    cursor.execute('''
        CREATE TABLE tiktoks (
            video_id TEXT PRIMARY KEY,
            author_id TEXT,
            author_username TEXT,
            author_name TEXT,
            description TEXT,
            created_at TEXT,
            likes INTEGER,
            plays INTEGER,
            shares INTEGER,
            comments INTEGER,
            search_keyword TEXT,
            category TEXT,
            relevance_score INTEGER,
            collected_at TEXT,
            platform TEXT,
            video_url TEXT
        )
    ''')
    conn.commit()
    print("Created new tiktoks table with platform column")
    return conn, cursor

def save_tiktok_to_db(cursor, tiktok_data, now, base_keyword, category, relevance_score):
    """Save individual TikTok to the database."""
    try:
        tiktok_tuple = (
            str(tiktok_data["id"]),
            str(tiktok_data["author"]["id"]) if "author" in tiktok_data else None,
            tiktok_data["author"]["uniqueId"] if "author" in tiktok_data else None,
            tiktok_data["author"]["nickname"] if "author" in tiktok_data else None,
            tiktok_data["desc"] if "desc" in tiktok_data else None,
            datetime.datetime.fromtimestamp(tiktok_data["createTime"]).isoformat() if "createTime" in tiktok_data else None,
            tiktok_data["stats"]["diggCount"] if "stats" in tiktok_data else 0,
            tiktok_data["stats"]["playCount"] if "stats" in tiktok_data else 0,
            tiktok_data["stats"]["shareCount"] if "stats" in tiktok_data else 0,
            tiktok_data["stats"]["commentCount"] if "stats" in tiktok_data else 0,
            base_keyword,
            category,
            relevance_score,
            now,
            "tiktok",
            f"https://www.tiktok.com/@{tiktok_data['author']['uniqueId']}/video/{tiktok_data['id']}"
        )

        cursor.execute('''
            INSERT INTO tiktoks
            (video_id, author_id, author_username, author_name, description, created_at,
            likes, plays, shares, comments, search_keyword, category, relevance_score,
            collected_at, platform, video_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', tiktok_tuple)
    except Exception as e:
        print(f"Error saving TikTok to database: {str(e)}")

async def main():
    try:
        async with TikTokApi() as api:
            ms_tokens = [
                "PTbnyQTiuEYbPzZBUxNmQfw8ABTGaDF0Akjw0UgEU5tILzRa4UVzRnQ-WZ8wm_brYwqKAOOZWsqD1TdWWgTOIwB_MsDkgpi_oByPo_BYUh15VhHQvyftutSnm4qspA2G_tm_-4VPlSYbGAU4k6L9ZRrQ",
                "JPGe0H6o0sUG0INJ1Bk656cD5RD40sniRXL5XVRzdWry54QDmv-ByPg2_nMcT9Z3Dep_dAE2syvvaie3IdkxxWnMJ32ajVb1jaFPNu1bghY8D9CdkciYAsR8tCcpUc23LoBiXSHrgB2TnxA8TKgfOIg="
            ]
            await api.create_sessions(num_sessions=2, ms_tokens=ms_tokens, headless=False, browser='webkit')
            print("Connected to TikTok API with sessions")

            category = input("Enter a category (e.g., fitness, cooking, tech): ").strip()
            keywords_input = input("Enter keywords to search for (separated by commas, e.g., python,coding): ").strip()
            keywords = [k.strip() for k in keywords_input.split(",") if k.strip()]

            if not keywords:
                print("No valid keywords entered. Exiting.")
                return

            supporting_keywords = {}
            for keyword in keywords:
                supporting_input = input(f"Enter supporting keywords for '{keyword}' (comma-separated, e.g., Artificial Intelligence, OpenAI): ").strip()
                supporting_list = [sk.strip() for sk in supporting_input.split(",") if sk.strip()]
                supporting_keywords[keyword] = [keyword.lower()] + [sk.lower() for sk in supporting_list]

            context_input = input("\nEnter additional context keywords to ensure tiktoks are relevant to your category\n"
                                  f"(e.g., for '{category}', enter words like 'workout,gym,health' separated by commas): ").strip()
            context_keywords = [k.strip().lower() for k in context_input.split(",") if k.strip()]

            print(f"\nSearching for the following keywords in category '{category}':")
            for idx, keyword in enumerate(keywords, 1):
                print(f"{idx}. {keyword} (supporting: {', '.join(supporting_keywords[keyword][1:])})")

            print("\nUsing the following context keywords to filter for relevance:")
            for idx, context in enumerate(context_keywords, 1):
                print(f"{idx}. {context}")

            conn, cursor = create_database()
            now = datetime.datetime.now().isoformat()
            max_tiktoks = 50

            for base_keyword in keywords:
                all_keywords_to_search = supporting_keywords[base_keyword]
                all_tiktoks_for_base = []
                for keyword in all_keywords_to_search:
                    all_tiktoks_for_base.extend(await fetch_tiktoks_for_keyword(api, keyword, max_tiktoks))

                print(f"\nProcessing {len(all_tiktoks_for_base)} tiktoks for base keyword '{base_keyword}'")
                filtered_author_count = 0
                filtered_context_count = 0
                saved_count = 0

                for tiktok_data, current_base_keyword in all_tiktoks_for_base:
                    author_username = tiktok_data["author"]["uniqueId"].lower() if "author" in tiktok_data and "uniqueId" in tiktok_data["author"] else ""
                    author_name = tiktok_data["author"]["nickname"].lower() if "author" in tiktok_data and "nickname" in tiktok_data["author"] else ""
                    author_contains_keyword = any(sk in author_username or sk in author_name for sk in supporting_keywords[current_base_keyword])

                    if author_contains_keyword:
                        filtered_author_count += 1
                        continue

                    description = tiktok_data["desc"].lower() if "desc" in tiktok_data else ""
                    if context_keywords:
                        relevance_score = sum(1 for ctx in context_keywords if ctx in description)
                        if relevance_score == 0:
                            filtered_context_count += 1
                            continue
                    else:
                        relevance_score = 1

                    save_tiktok_to_db(cursor, tiktok_data, now, base_keyword, category, relevance_score)
                    saved_count += 1

                conn.commit()
                print(f"Saved {saved_count} tiktoks for base keyword '{base_keyword}' to database")
                print(f"Filtered out {filtered_author_count} tiktoks due to author name/username containing related keywords")
                print(f"Filtered out {filtered_context_count} tiktoks due to lack of category context")

            cursor.execute("SELECT COUNT(*) FROM tiktoks")
            total_tiktoks = cursor.fetchone()[0]
            print(f"\nTotal tiktoks in database: {total_tiktoks}")

            if total_tiktoks > 0:
                cursor.execute("SELECT search_keyword, COUNT(*), AVG(relevance_score) FROM tiktoks GROUP BY search_keyword")
                keyword_stats = cursor.fetchall()
                print(f"\nTiktoks in category '{category}' by base keyword:")
                for keyword, count, avg_relevance in keyword_stats:
                    print(f"  {keyword}: {count} tiktoks (avg relevance: {avg_relevance:.1f})")

                print("\nExamples of high-relevance tiktoks:")
                cursor.execute("""
                    SELECT search_keyword, description, relevance_score, video_url
                    FROM tiktoks
                    ORDER BY relevance_score DESC
                    LIMIT 3
                """)
                examples = cursor.fetchall()
                for i, (keyword, desc, score, url) in enumerate(examples, 1):
                    short_desc = desc[:100] + "..." if len(desc) > 100 else desc
                    print(f"\n{i}. Base Keyword '{keyword}' (relevance: {score}):\n   {short_desc}\n   URL: {url}")

            conn.close()
            print("\nSearch completed and fresh tiktoks aggregated to SQLite database")
    except Exception as e:
        print(f"Failed to initialize TikTok API: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
