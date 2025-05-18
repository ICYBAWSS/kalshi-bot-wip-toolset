import os
import subprocess

# Define absolute paths to all scripts
script_dir = '/Users/ray/kalshi-bot-env/'
twitter_script = os.path.join(script_dir, 'twitter.py')
tiktok_script = os.path.join(script_dir, 'tiktok.py')
sentiment_script = os.path.join(script_dir, 'sentiment_analysis.py')

# Get all inputs from user once
category = input("Enter a category (e.g., politics, sports, music): ").strip()
keywords_input = input("Enter keywords to search for (separated by commas): ").strip()
primary_keywords = [k.strip() for k in keywords_input.split(",") if k.strip()]

# Prepare a dictionary to store supporting keywords for each primary keyword
supporting_keywords_map = {}
for pk in primary_keywords:
    supporting_input = input(f"Enter supporting keywords for '{pk}' (comma-separated, e.g., Artificial Intelligence, OpenAI): ").strip()
    supporting_keywords_map[pk] = supporting_input.strip()  # Store as comma-separated string

context_input = input(f"Enter additional context keywords for '{category}' (comma-separated): ").strip()

# Prepare the input for twitter_huge.py
twitter_huge_input = f"{category}\n{keywords_input}\n"
for pk in primary_keywords:
    twitter_huge_input += f"{supporting_keywords_map[pk]}\n"
twitter_huge_input += f"{context_input}\n"

# Run twitter_huge.py with full path
print("\n--- Running twitter_huge.py ---")
try:
    subprocess.run(['python3', twitter_script], input=twitter_huge_input.encode(), check=True)
except subprocess.CalledProcessError as e:
    print(f"Error running twitter script: {e}")
    print(f"Checking if file exists: {os.path.exists(twitter_script)}")

# Prepare the input for tiktok_huge.py
tiktok_huge_input = f"{category}\n{keywords_input}\n"
for pk in primary_keywords:
    tiktok_huge_input += f"{supporting_keywords_map[pk]}\n"
tiktok_huge_input += f"{context_input}\n"

# Run tiktok_huge.py with full path
print("\n--- Running tiktok_huge.py ---")
try:
    subprocess.run(['python3', tiktok_script], input=tiktok_huge_input.encode(), check=True)
except subprocess.CalledProcessError as e:
    print(f"Error running tiktok script: {e}")

# Run sentiment_analysis.py with full path
print("\n--- Running sentiment_analysis.py ---")
try:
    subprocess.run(['python3', sentiment_script], check=True)
except subprocess.CalledProcessError as e:
    print(f"Error running sentiment analysis script: {e}")

