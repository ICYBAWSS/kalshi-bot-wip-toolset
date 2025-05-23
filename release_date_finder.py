import asyncio
import datetime
import re
import logging
import json
import os
from typing import Dict, List, Tuple, Optional, Set
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode

# Configure logging
logging.basicConfig(
    filename='release_dates.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# File to store cached release dates
CACHE_FILE = 'release_dates_cache.json'

# Global cache
release_date_cache: Dict[str, str] = {}

async def check_wikipedia(crawler, config, title, artist):
    """Check Wikipedia for release date."""
    print(f"\nChecking Wikipedia for {title} by {artist}...")
    try:
        # Try specific song page with artist name first (most accurate)
        wiki_url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}_{artist.replace(' ', '_')}_song"
        print(f"Trying specific song Wikipedia URL: {wiki_url}")
        wiki_result = await crawler.arun(url=wiki_url, config=config)
        
        if wiki_result and wiki_result.markdown:
            wiki_content = wiki_result.markdown
            
            # Look for song release date in Wikipedia content
            release_date = extract_release_date_from_wiki_content(wiki_content, True)
            if release_date:
                return release_date
        
        # Try song page without artist name
        wiki_url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
        print(f"Trying song-only Wikipedia URL: {wiki_url}")
        wiki_result = await crawler.arun(url=wiki_url, config=config)
        
        if wiki_result and wiki_result.markdown:
            wiki_content = wiki_result.markdown
            
            # Verify the page is about the right song by checking for artist name
            if re.search(rf"\b{re.escape(artist)}\b", wiki_content, re.IGNORECASE):
                print(f"Found artist name in song page, extracting release date")
                release_date = extract_release_date_from_wiki_content(wiki_content, True)
                if release_date:
                    return release_date
            else:
                print(f"Artist name not found in song page, might be wrong song")
        
        # Try artist's Wikipedia page as fallback
        artist_wiki_url = f"https://en.wikipedia.org/wiki/{artist.replace(' ', '_')}"
        print(f"Trying artist Wikipedia URL: {artist_wiki_url}")
        artist_wiki_result = await crawler.arun(url=artist_wiki_url, config=config)
        
        if artist_wiki_result and artist_wiki_result.markdown:
            artist_wiki_content = artist_wiki_result.markdown
            
            # Look for the song name in the artist's page
            song_section_match = re.search(f"(?i).*{re.escape(title)}.*", artist_wiki_content)
            if song_section_match:
                # Extract a section around the song mention
                section_start = max(0, song_section_match.start() - 1000)
                section_end = min(len(artist_wiki_content), song_section_match.end() + 1000)
                relevant_section = artist_wiki_content[section_start:section_end]
                
                print(f"Found song mention in artist page, extracting release date from surrounding context")
                # Look for release date near the song mention
                release_date = extract_release_date_from_wiki_content(relevant_section, False)
                if release_date:
                    return release_date
    except Exception as e:
        print(f"Error with Wikipedia: {e}")
    
    return None

async def check_youtube(crawler, config, title, artist):
    """Check YouTube for release date."""
    print(f"\nChecking YouTube for {title} by {artist}...")
    try:
        # Prefer "official audio" in search
        search_url = f"https://www.youtube.com/results?search_query={title.replace(' ', '+')}+{artist.replace(' ', '+')}+official+audio"
        print(f"Searching YouTube (official audio): {search_url}")
        search_result = await crawler.arun(url=search_url, config=config)
        
        if search_result and search_result.markdown:
            # Look for video links in search results
            video_links = re.findall(r'href="(/watch\?v=[^"&]+)"', search_result.markdown)
            
            # If no "official audio" found, try "official video"
            if not video_links:
                search_url = f"https://www.youtube.com/results?search_query={title.replace(' ', '+')}+{artist.replace(' ', '+')}+official+video"
                print(f"Searching YouTube (official video): {search_url}")
                search_result = await crawler.arun(url=search_url, config=config)
                if search_result and search_result.markdown:
                    video_links = re.findall(r'href="(/watch\?v=[^"&]+)"', search_result.markdown)
            
            if video_links:
                # Visit the first result
                video_url = f"https://www.youtube.com{video_links[0]}"
                print(f"Visiting YouTube video: {video_url}")
                video_result = await crawler.arun(url=video_url, config=config)
                
                if video_result and video_result.markdown:
                    # Look for upload date
                    date_match = re.search(r'(?:Published|Uploaded|Premiered)[:\s]+([A-Za-z]+ \d{1,2}, \d{4}|\d{4})', 
                                          video_result.markdown, re.IGNORECASE)
                    if date_match:
                        date_str = date_match.group(1)
                        print(f"Found YouTube date: {date_str}")
                        
                        # Try to parse the date
                        try:
                            # Try different formats
                            for fmt in ['%B %d, %Y', '%Y']:
                                try:
                                    date_obj = datetime.datetime.strptime(date_str, fmt)
                                    formatted_date = date_obj.strftime('%Y-%m-%d')
                                    print(f"Parsed YouTube date: {formatted_date}")
                                    return formatted_date
                                except ValueError:
                                    continue
                            
                            # If just a year was found
                            if re.match(r'^\d{4}$', date_str):
                                return f"{date_str}-01-01"
                        except Exception as e:
                            print(f"Error parsing YouTube date: {e}")
    except Exception as e:
        print(f"Error with YouTube: {e}")
    
    return None

async def check_spotify(crawler, config, title, artist):
    """Check Spotify for release date."""
    print(f"\nChecking Spotify for {title} by {artist}...")
    try:
        # Search using Google since direct Spotify search might not work well
        search_url = f"https://www.google.com/search?q=site:open.spotify.com+{title.replace(' ', '+')}+{artist.replace(' ', '+')}"
        print(f"Searching for Spotify links: {search_url}")
        search_result = await crawler.arun(url=search_url, config=config)
        
        if search_result and search_result.markdown:
            # Look for Spotify track links in search results
            spotify_links = re.findall(r'(https://open.spotify.com/track/[^"&\s]+)', search_result.markdown)
            
            if spotify_links:
                # Visit the first result
                spotify_url = spotify_links[0]
                print(f"Visiting Spotify track page: {spotify_url}")
                spotify_result = await crawler.arun(url=spotify_url, config=config)
                
                if spotify_result and spotify_result.markdown:
                    # Look for release date
                    date_match = re.search(r'(?:release date|released)[:\s]+([A-Za-z]+ \d{1,2}, \d{4}|\d{1,2} [A-Za-z]+ \d{4}|\d{4})', 
                                          spotify_result.markdown, re.IGNORECASE)
                    if date_match:
                        date_str = date_match.group(1)
                        print(f"Found Spotify date: {date_str}")
                        
                        # Try to parse the date
                        try:
                            # Try different formats
                            for fmt in ['%B %d, %Y', '%d %B %Y', '%Y']:
                                try:
                                    date_obj = datetime.datetime.strptime(date_str, fmt)
                                    formatted_date = date_obj.strftime('%Y-%m-%d')
                                    print(f"Parsed Spotify date: {formatted_date}")
                                    return formatted_date
                                except ValueError:
                                    continue
                            
                            # If just a year was found
                            if re.match(r'^\d{4}$', date_str):
                                return f"{date_str}-01-01"
                        except Exception as e:
                            print(f"Error parsing Spotify date: {e}")
    except Exception as e:
        print(f"Error with Spotify: {e}")
    
    return None

async def check_apple_music(crawler, config, title, artist):
    """Check Apple Music for release date."""
    print(f"\nChecking Apple Music for {title} by {artist}...")
    try:
        # Search for the song
        search_url = f"https://music.apple.com/us/search?term={title.replace(' ', '+')}+{artist.replace(' ', '+')}"
        print(f"Searching Apple Music: {search_url}")
        search_result = await crawler.arun(url=search_url, config=config)
        
        if search_result and search_result.markdown:
            # Look for song links in search results
            song_links = re.findall(r'href="(https://music.apple.com/us/[^"]+)"', search_result.markdown)
            
            # Filter for links that seem to be songs
            song_links = [link for link in song_links if '/song/' in link]
            
            if song_links:
                # Visit the first result
                song_url = song_links[0]
                print(f"Visiting Apple Music song page: {song_url}")
                song_result = await crawler.arun(url=song_url, config=config)
                
                if song_result and song_result.markdown:
                    # Look for release date
                    date_match = re.search(r'(?:RELEASED|Release Date)[:\s]+([A-Za-z]+ \d{1,2}, \d{4}|\d{4})', 
                                          song_result.markdown, re.IGNORECASE)
                    if date_match:
                        date_str = date_match.group(1)
                        print(f"Found Apple Music date: {date_str}")
                        
                        # Try to parse the date
                        try:
                            # Try different formats
                            for fmt in ['%B %d, %Y', '%Y']:
                                try:
                                    date_obj = datetime.datetime.strptime(date_str, fmt)
                                    formatted_date = date_obj.strftime('%Y-%m-%d')
                                    print(f"Parsed Apple Music date: {formatted_date}")
                                    return formatted_date
                                except ValueError:
                                    continue
                            
                            # If just a year was found
                            if re.match(r'^\d{4}$', date_str):
                                return f"{date_str}-01-01"
                        except Exception as e:
                            print(f"Error parsing Apple Music date: {e}")
    except Exception as e:
        print(f"Error with Apple Music: {e}")
    
    return None

def extract_release_date_from_wiki_content(content, is_song_page):
    """
    Extract release date from Wikipedia content.
    
    Args:
        content (str): Wikipedia page content
        is_song_page (bool): Whether this is a dedicated song page
        
    Returns:
        str: Release date in YYYY-MM-DD format or empty string if not found
    """
    # Patterns specifically for song release dates
    song_release_patterns = [
        r'[Rr]eleased\s*:?\s*(\d{1,2} [A-Z][a-z]+ \d{4})',  # Released: DD Month YYYY
        r'[Rr]eleased\s*:?\s*([A-Z][a-z]+ \d{1,2},? \d{4})',  # Released: Month DD, YYYY
        r'[Rr]elease date\s*:?\s*(\d{1,2} [A-Z][a-z]+ \d{4})',  # Release date: DD Month YYYY
        r'[Rr]elease date\s*:?\s*([A-Z][a-z]+ \d{1,2},? \d{4})',  # Release date: Month DD, YYYY
        r'[Rr]eleased in\s*:?\s*(\d{4})',  # Released in YYYY
        r'[Rr]eleased\s*:?\s*(\d{4})',  # Released: YYYY
        r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
    ]
    
    # First look for patterns that specifically mention "single" or "song"
    specific_patterns = [
        r'[Ss]ingle\s+[Rr]eleased\s*:?\s*(\d{1,2} [A-Z][a-z]+ \d{4})',  # Single Released: DD Month YYYY
        r'[Ss]ingle\s+[Rr]eleased\s*:?\s*([A-Z][a-z]+ \d{1,2},? \d{4})',  # Single Released: Month DD, YYYY
        r'[Ss]ong\s+[Rr]eleased\s*:?\s*(\d{1,2} [A-Z][a-z]+ \d{4})',  # Song Released: DD Month YYYY
        r'[Ss]ong\s+[Rr]eleased\s*:?\s*([A-Z][a-z]+ \d{1,2},? \d{4})',  # Song Released: Month DD, YYYY
        r'[Ss]ingle\s+[Rr]elease date\s*:?\s*(\d{1,2} [A-Z][a-z]+ \d{4})',  # Single Release date: DD Month YYYY
        r'[Ss]ingle\s+[Rr]elease date\s*:?\s*([A-Z][a-z]+ \d{1,2},? \d{4})',  # Single Release date: Month DD, YYYY
    ]
    
    # If this is a dedicated song page, we can be more confident about general release patterns
    patterns_to_try = specific_patterns + (song_release_patterns if is_song_page else [])
    
    for pattern in patterns_to_try:
        matches = re.findall(pattern, content)
        if matches:
            date_str = matches[0]
            print(f"Found potential release date: {date_str}")
            
            # Try to convert to YYYY-MM-DD format
            try:
                # Try different date formats
                for fmt in ['%Y-%m-%d', '%B %d, %Y', '%B %d %Y', '%d %B %Y']:
                    try:
                        date_obj = datetime.datetime.strptime(date_str, fmt)
                        
                        # Validate the date is not in the future
                        current_date = datetime.datetime.now()
                        if date_obj > current_date:
                            print(f"Found future date: {date_str}. This is likely incorrect.")
                            continue
                        
                        formatted_date = date_obj.strftime('%Y-%m-%d')
                        print(f"Converted to: {formatted_date}")
                        return formatted_date
                    except ValueError:
                        continue
                
                # If we couldn't parse but found a year
                year_match = re.search(r'(\d{4})', date_str)
                if year_match:
                    year = year_match.group(1)
                    # Validate the year is not in the future
                    current_year = datetime.datetime.now().year
                    if int(year) <= current_year:
                        formatted_date = f"{year}-01-01"
                        print(f"Using year only: {formatted_date}")
                        return formatted_date
            except Exception as e:
                print(f"Error parsing date: {e}")
    
    # If we're on a song page but didn't find specific patterns, try looking for any year in an infobox
    if is_song_page:
        # Look for infobox section which typically contains release info
        infobox_match = re.search(r'(?i)infobox.*?released', content)
        if infobox_match:
            # Extract a section around the infobox release mention
            section_start = max(0, infobox_match.start() - 100)
            section_end = min(len(content), infobox_match.end() + 200)
            infobox_section = content[section_start:section_end]
            
            # Look for years in this section
            year_matches = re.findall(r'\b(19\d{2}|20\d{2})\b', infobox_section)
            if year_matches:
                # Use the first year that's not in the future
                current_year = datetime.datetime.now().year
                for year in year_matches:
                    if int(year) <= current_year:
                        print(f"Found year in infobox: {year}")
                        return f"{year}-01-01"
    
    return ""

async def get_release_date(title: str, artist: str) -> str:
    """
    Get release date using multiple sources with parallel processing.
    
    Args:
        title (str): Song title
        artist (str): Artist name
        
    Returns:
        str: Release date in YYYY-MM-DD format or empty string if not found
    """
    # Generate cache key
    track_key = f"{title}|{artist}"
    
    # Check cache first
    if track_key in release_date_cache:
        print(f"Using cached release date for {track_key}: {release_date_cache[track_key]}")
        return release_date_cache[track_key]
    
    print(f"\nGetting release date for: {title} by {artist}")
    
    # Clean up the title and artist for better search results
    clean_title = re.sub(r'[\(\[].*?[\)\]]', '', title).strip()  # Remove text in parentheses
    clean_artist = artist.split(',')[0].split('&')[0].strip()  # Take first artist if multiple
    
    print(f"Cleaned search terms: title='{clean_title}', artist='{clean_artist}'")
    
    # Store all found dates for voting
    all_dates = []
    
    try:
        async with AsyncWebCrawler() as crawler:
            config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,  # Don't use cache
                wait_until='domcontentloaded',  # Less strict wait condition to avoid timeouts
                verbose=True,  # Enable verbose logging
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
                page_timeout=30000  # 30 seconds timeout
            )
            
            # Define sources to check
            sources = [
                ("Wikipedia", check_wikipedia),
                ("YouTube", check_youtube),
                ("Apple Music", check_apple_music),
                ("Spotify", check_spotify),
            ]
            
            # Check all sources in parallel
            tasks = [check_function(crawler, config, clean_title, clean_artist) 
                    for source_name, check_function in sources]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for i, result in enumerate(results):
                source_name = sources[i][0]
                if isinstance(result, Exception):
                    print(f"Error checking {source_name}: {result}")
                elif result:
                    all_dates.append((source_name, result))
                    print(f"Found date from {source_name}: {result}")
            
            # Print all found dates
            print("\nDates found from different sources:")
            for source, date in all_dates:
                print(f"- {source}: {date}")
            
            # If we have dates, choose the best one
            if all_dates:
                # Use majority voting if possible
                date_counts = {}
                for _, date in all_dates:
                    # Only count the year part for voting
                    year = date.split('-')[0]
                    date_counts[year] = date_counts.get(year, 0) + 1
                
                # Find the most common year
                most_common_year = max(date_counts.items(), key=lambda x: x[1])[0]
                print(f"Most common year from voting: {most_common_year}")
                
                # Find the most specific date with that year
                best_date = None
                for _, date in all_dates:
                    if date.startswith(most_common_year):
                        # Prefer more specific dates (not just year-01-01)
                        if not best_date or (date != f"{most_common_year}-01-01" and best_date == f"{most_common_year}-01-01"):
                            best_date = date
                
                if best_date:
                    print(f"Selected best date: {best_date}")
                    # Validate the release date - ensure it's not in the future
                    try:
                        release_date_obj = datetime.datetime.strptime(best_date, '%Y-%m-%d')
                        current_date = datetime.datetime.now()
                        
                        # If the release date is in the future, it's likely incorrect
                        if release_date_obj > current_date:
                            print(f"⚠️ Detected future release date: {best_date}. This is likely incorrect.")
                            # Use just the year if it's valid
                            year_match = re.search(r'(\d{4})', best_date)
                            if year_match:
                                year = year_match.group(1)
                                # If year is also in the future, use empty string
                                if int(year) > current_date.year:
                                    print("Future year detected, leaving date blank")
                                    best_date = ""
                                else:
                                    corrected_date = f"{year}-01-01"
                                    print(f"Corrected to: {corrected_date}")
                                    best_date = corrected_date
                            else:
                                # If we can't extract a year, use empty string
                                print("Could not correct the date. Using empty string.")
                                best_date = ""
                    except Exception as e:
                        print(f"Error validating release date: {e}")
                    
                    # Cache the result
                    release_date_cache[track_key] = best_date
                    return best_date
                
                # Fallback to the first date if no best date was found
                first_date = all_dates[0][1]
                print(f"Using first found date: {first_date}")
                release_date_cache[track_key] = first_date
                return first_date
    
    except Exception as e:
        print(f"Error getting release date: {e}")
        logging.error(f"Error getting release date: {e}", exc_info=True)
    
    # If all else failed, try a simpler approach - extract year from title or artist context
    print("\nTrying to extract year from title or artist context...")
    year_match = re.search(r'(19\d{2}|20\d{2})', f"{title} {artist}")
    if year_match:
        year = year_match.group(1)
        # Ensure the year is not in the future
        current_year = datetime.datetime.now().year
        if int(year) > current_year:
            print(f"Found future year in context, leaving date blank")
            release_date_cache[track_key] = ""
            return ""
        
        print(f"Extracted year from context: {year}")
        release_date = f"{year}-01-01"
        
        # Cache this result too
        release_date_cache[track_key] = release_date
        return release_date
    
    # If everything failed, leave blank
    print(f"✗ Could not determine release date from any source. Leaving blank.")
    release_date_cache[track_key] = ""
    return ""

def load_cache():
    """Load the release date cache from file."""
    global release_date_cache
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'r') as f:
                release_date_cache = json.load(f)
                print(f"Loaded {len(release_date_cache)} cached release dates")
    except Exception as e:
        print(f"Error loading cache: {e}")
        release_date_cache = {}

def save_cache():
    """Save the release date cache to file."""
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(release_date_cache, f)
            print(f"Saved {len(release_date_cache)} release dates to cache")
    except Exception as e:
        print(f"Error saving cache: {e}")

async def process_batch(songs: List[Tuple[str, str]]) -> Dict[str, str]:
    """
    Process a batch of songs in parallel to get their release dates.
    
    Args:
        songs: List of (title, artist) tuples
        
    Returns:
        Dict mapping "title|artist" to release date
    """
    tasks = [get_release_date(title, artist) for title, artist in songs]
    results = await asyncio.gather(*tasks)
    
    return {f"{title}|{artist}": date for (title, artist), date in zip(songs, results)}

async def main_async():
    """Main async function to process songs from input file."""
    # Load existing cache
    load_cache()
    
    # Ask for input file
    input_file = input("Enter the path to the input file (CSV format with title,artist): ")
    if not input_file:
        input_file = "songs_to_process.csv"
        print(f"Using default input file: {input_file}")
    
    # Check if file exists, if not create a sample
    if not os.path.exists(input_file):
        print(f"File {input_file} not found. Creating a sample file...")
        with open(input_file, 'w') as f:
            f.write("title,artist\n")
            f.write("Die With A Smile,Lady Gaga, Bruno Mars\n")
            f.write("Espresso,Sabrina Carpenter\n")
            f.write("Texas Hold 'Em,Beyoncé\n")
        print(f"Sample file created. Please add your songs to {input_file} and run again.")
        return
    
    # Read songs from file
    songs_to_process = []
    try:
        with open(input_file, 'r') as f:
            # Skip header
            header = f.readline()
            for line in f:
                if line.strip():
                    parts = line.strip().split(',', 1)
                    if len(parts) == 2:
                        title, artist = parts
                        songs_to_process.append((title.strip(), artist.strip()))
    except Exception as e:
        print(f"Error reading input file: {e}")
        return
    
    print(f"Found {len(songs_to_process)} songs to process")
    
    # Ask for batch size
    try:
        batch_size_input = input("Enter batch size for parallel processing (default: 5): ")
        batch_size = int(batch_size_input) if batch_size_input.strip() else 5
    except ValueError:
        print("Invalid input. Using default batch size of 5.")
        batch_size = 5
    
    # Process in batches
    results = {}
    for i in range(0, len(songs_to_process), batch_size):
        batch = songs_to_process[i:i+batch_size]
        print(f"\nProcessing batch {i//batch_size + 1}/{(len(songs_to_process) + batch_size - 1)//batch_size}")
        batch_results = await process_batch(batch)
        results.update(batch_results)
        
        # Save cache after each batch
        save_cache()
    
    # Write results to output file
    output_file = input_file.replace('.csv', '_with_dates.csv')
    try:
        with open(output_file, 'w') as f:
            f.write("title,artist,release_date\n")
            for title, artist in songs_to_process:
                key = f"{title}|{artist}"
                date = results.get(key, "")
                f.write(f"{title},{artist},{date}\n")
        print(f"\nResults written to {output_file}")
    except Exception as e:
        print(f"Error writing output file: {e}")
    
    # Final cache save
    save_cache()
    print("Processing complete!")

def main():
    """Main function to run the async process."""
    try:
        print("\n" + "="*50)
        print("RELEASE DATE FINDER - PARALLEL PROCESSING".center(50))
        print("="*50 + "\n")
        
        asyncio.run(main_async())
        
    except KeyboardInterrupt:
        print("\n\nScript interrupted by user. Saving cache and exiting...")
        save_cache()
    except Exception as e:
        print(f"Error in main function: {e}")
        logging.error(f"Error in main function: {e}", exc_info=True)
    finally:
        print("\n" + "="*50)
        print("PROCESSING COMPLETE".center(50))
        print("="*50)

if __name__ == "__main__":
    main()
