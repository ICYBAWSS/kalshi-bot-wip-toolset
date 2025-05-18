from tweety import Twitter
import random
import time
import os

def test_twitter_search():
    """
    Test script to verify Twitter search functionality with different keywords
    to diagnose issues with search terms like "trump"
    """
    print("Twitter Search Test - Diagnostics")
    print("=================================")
    
    # Connect to existing session
    app = Twitter("twitter_session")
    session_file = "twitter_session.tw_session"
    
    if not os.path.exists(session_file):
        print(f"ERROR: Session file {session_file} not found!")
        print("Please run the authentication refresher script first.")
        return
    
    try:
        app.connect()
        print(f"Connected to Twitter session as: {app.me.username}")
    except Exception as e:
        print(f"Failed to connect to session: {e}")
        print("Please run the authentication refresher script first.")
        return
    
    # Test keywords - from safe to potentially restricted
    test_keywords = [
        "weather",       # Very safe keyword
        "food",          # Safe keyword
        "news",          # Common keyword
        "politics",      # Potentially sensitive topic
        "biden",         # Political figure
        "trump"          # Your problem keyword
    ]
    
    results = {}
    
    print("\nTesting search with various keywords...\n")
    for keyword in test_keywords:
        print(f"Testing search for '{keyword}'...")
        try:
            start_time = time.time()
            tweets = app.search(keyword, pages=1)  # Only try to get first page
            end_time = time.time()
            
            tweet_count = len(tweets)
            results[keyword] = {
                "success": True,
                "count": tweet_count,
                "time": end_time - start_time
            }
            
            print(f"✓ Success! Found {tweet_count} tweets in {results[keyword]['time']:.2f} seconds")
            
            # Show a sample tweet if available
            if tweets:
                sample = random.choice(tweets)
                print(f"  Sample: \"{sample.text[:60]}...\"")
            
        except Exception as e:
            results[keyword] = {
                "success": False,
                "error": str(e)
            }
            print(f"✗ Failed: {e}")
        
        # Add a delay between searches
        delay = random.uniform(2, 4)
        print(f"Waiting {delay:.1f} seconds before next search...\n")
        time.sleep(delay)
    
    # Summary report
    print("\n=== Search Test Results ===")
    print("Keyword".ljust(12) + "Status".ljust(10) + "Results".ljust(10) + "Time(s)".ljust(10) + "Notes")
    print("-" * 70)
    
    for keyword, data in results.items():
        if data["success"]:
            status = "✓ OK"
            result_count = str(data["count"])
            time_taken = f"{data['time']:.2f}"
            notes = ""
        else:
            status = "✗ FAILED"
            result_count = "0"
            time_taken = "N/A"
            notes = data["error"][:30]
            
        print(f"{keyword.ljust(12)}{status.ljust(10)}{result_count.ljust(10)}{time_taken.ljust(10)}{notes}")
    
    # Analysis and recommendations
    print("\n=== Analysis ===")
    
    all_success = all(data["success"] for data in results.values())
    if all_success:
        print("✓ All searches are working correctly!")
        print("  If you were previously having issues, they appear to be resolved.")
    else:
        failing_keywords = [k for k, v in results.items() if not v["success"]]
        working_keywords = [k for k, v in results.items() if v["success"]]
        
        print(f"✗ Some searches are failing: {', '.join(failing_keywords)}")
        
        if working_keywords:
            print(f"✓ These searches are working: {', '.join(working_keywords)}")
            
        # Specific recommendations
        if "trump" in failing_keywords and len(failing_keywords) == 1:
            print("\nRecommendations:")
            print("1. Try using alternative keywords like 'president' or 'election'")
            print("2. Break your search into more specific terms like 'donald policy' or 'republican campaign'")
            print("3. Your Twitter account might have search restrictions - consider creating a new account")
            print("4. Try using the official Twitter API instead of web scraping if possible")
        elif len(failing_keywords) > 1:
            print("\nMultiple search terms are failing. This suggests a possible account limitation.")
            print("Recommendations:")
            print("1. Create a new Twitter account specifically for scraping")
            print("2. Wait 24 hours before trying again (possible temporary limitation)")
            print("3. Consider using the official Twitter API instead of web scraping")

if __name__ == "__main__":
    test_twitter_search()