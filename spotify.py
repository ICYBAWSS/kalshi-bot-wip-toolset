import sqlite3
import logging
import os
import datetime
import requests
import json
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
from tqdm import tqdm

# Configure logging
logging.basicConfig(filename='spotify_scraper.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Login credentials
EMAIL = "spotify765756@gmail.com"  # Your Spotify email
PASSWORD = "Poop12345#"            # Your Spotify password

# Backup login credentials
BACKUP_EMAIL = "rofeto2117@cotigz.com"
BACKUP_PASSWORD = "KirklandA1"

# Cache for API calls to avoid duplicate requests
musicbrainz_cache = {}

def setup_driver():
    """Set up and return the Chrome WebDriver."""
    print("Setting up Chrome WebDriver...")
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')  # Uncomment when not debugging
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    print("WebDriver setup complete.")
    return driver

def login_to_spotify(driver, email, password, is_backup=False):
    """Login to Spotify using the password flow with robust fallback options."""
    try:
        print(f"Attempting to login with {'backup' if is_backup else 'primary'} credentials...")
        logging.info(f"Attempting to login with {'backup' if is_backup else 'primary'} credentials...")
        driver.get("https://charts.spotify.com")

        print("Waiting for login link...")
        logging.info("Waiting for login link...")
        login_link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a[data-testid='charts-login']"))
        )
        login_link.click()
        print("Clicked login link.")
        time.sleep(2)

        # Enter email first
        print("Entering email...")
        logging.info("Entering email...")
        email_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[data-testid='login-username']"))
        )
        email_field.send_keys(email)
        
        # Click continue button
        print("Clicking continue button...")
        continue_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-testid='login-button']"))
        )
        continue_button.click()
        logging.info("Clicked continue after entering email")
        time.sleep(3)  # Give more time for the next page to load

        # Try multiple approaches to find the password field
        try:
            print("Checking for direct password field...")
            password_field = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[data-testid='login-password']"))
            )
            print("Password field found directly.")
            password_field.send_keys(password)
            
            # Click login button
            login_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-testid='login-button']"))
            )
            login_button.click()
            logging.info("Direct password entry successful")
            
        except Exception as e:
            print(f"Direct password field not found: {e}")
            
            # Look for "Log in with password" option
            try:
                print("Looking for 'Log in with password' option...")
                password_options = driver.find_elements(By.XPATH, "//*[contains(text(), 'password')]")
                if password_options:
                    for option in password_options:
                        if option.is_displayed():
                            print(f"Found password option: {option.text}")
                            driver.execute_script("arguments[0].click();", option)
                            time.sleep(2)
                            break
                
                # Now try to find password field again
                password_field = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password']"))
                )
                password_field.send_keys(password)
                
                # Find submit/login button
                submit_buttons = driver.find_elements(By.XPATH, "//button[@type='log in']")
                if submit_buttons:
                    submit_buttons[0].click()
                else:
                    # Try to find by text
                    login_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-testid='login-button']"))
                    )
                    login_button.click()
            
            except Exception as e:
                print(f"Failed to handle password entry: {e}")
                raise
        
        # Wait until logged in and redirected to charts
        print("Waiting for successful login redirection...")
        try:
            WebDriverWait(driver, 30).until(
                EC.url_contains("charts.spotify.com/charts")
            )
            print("Login successful! ✓")
            logging.info("Login successful")
            return True
        except:
            print("Login timeout or failed. Checking current URL...")
            print(f"Current URL: {driver.current_url}")
            
            if "charts.spotify.com" in driver.current_url:
                print("URL contains charts.spotify.com - assuming login successful despite timeout")
                logging.info("Login successful (detected by URL)")
                return True
            else:
                if not is_backup:
                    print("Primary login failed. Trying backup credentials...")
                    # Clear cookies and try again with backup credentials
                    driver.delete_all_cookies()
                    return login_to_spotify(driver, BACKUP_EMAIL, BACKUP_PASSWORD, True)
                else:
                    raise Exception("Login failed with both primary and backup credentials")
        
    except Exception as e:
        print(f"❌ Login failed: {e}")
        logging.error(f"Login failed: {e}", exc_info=True)
        
        if not is_backup:
            print("Trying backup credentials after exception...")
            # Clear cookies and try again with backup credentials
            driver.delete_all_cookies()
            return login_to_spotify(driver, BACKUP_EMAIL, BACKUP_PASSWORD, True)
        else:
            raise Exception("Login failed with both primary and backup credentials")

def scrape_spotify_charts(driver, url, chart_date, chart_type, db_connection):
    """
    Scrape chart data from top 20 entries of Spotify Charts.
    
    Args:
        driver (webdriver.Chrome): The Selenium WebDriver instance.
        url (str): The URL of the Spotify chart.
        chart_date (str): The date of the chart ('latest' or 'YYYY-MM-DD').
        chart_type (str): The type of chart (e.g., 'global', 'usa').
        db_connection: Database connection to save entries incrementally.

    Returns:
        list: A list of dictionaries, where each dictionary represents a song entry.
    """
    try:
        # Navigate to the chart URL first
        print(f"Navigating to chart URL: {url}")
        driver.get(url)
        time.sleep(5)  # Give page time to load

        # Wait for chart to load - wait for table rows to appear
        print("Waiting for chart data to load...")
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
        )
        
        # Handle date determination
        display_date = chart_date
        if chart_date == 'latest':
            # Try to find date display element
            try:
                date_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='date-picker-trigger']"))
                )
                actual_date = date_element.text.strip()
                print(f"Found date on page: '{actual_date}'")
                
                # Try to parse the date
                try:
                    # Try different date formats
                    for fmt in ["%B %d, %Y", "%b %d, %Y", "%d %b %Y"]:
                        try:
                            date_obj = datetime.datetime.strptime(actual_date, fmt)
                            display_date = date_obj.strftime('%Y-%m-%d')
                            print(f"Parsed date as: {display_date}")
                            break
                        except ValueError:
                            continue
                except:
                    # If parsing fails, use current date
                    display_date = datetime.datetime.now().strftime('%Y-%m-%d')
            except:
                # If we can't find the date element, use current date
                display_date = datetime.datetime.now().strftime('%Y-%m-%d')
                print(f"Using current date: {display_date}")
        
        print(f"\nScraping top 20 entries from {chart_type} chart for {display_date}")
        logging.info(f"Scraping chart for date: {display_date}")
        
        # Get all chart rows
        chart_rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        
        # Only take the top 20 rows
        top_20_rows = chart_rows[:20]
        
        print(f"Found {len(top_20_rows)} rows for top 20 chart entries")
        logging.info(f"Found {len(top_20_rows)} rows for top 20 chart entries")

        # Process the rows
        chart_data = []
        for i, row in enumerate(top_20_rows):
            print(f"Processing entry #{i+1}...")
            
            # Take screenshot of this row
            driver.execute_script("arguments[0].scrollIntoView();", row)
            time.sleep(0.5)
            
            try:
                # Get the HTML content of the row for more detailed parsing
                row_html = row.get_attribute('outerHTML')
                print(f"Row HTML: {row_html[:200]}...")  # Print first 200 chars for debugging
                
                # Get all cells in the row
                cells = row.find_elements(By.TAG_NAME, "td")
                
                if not cells or len(cells) < 3:
                    print(f"Not enough cells found in row {i+1}, skipping...")
                    continue
                
                # Extract position (first cell)
                position_cell = cells[0]
                position_text = position_cell.text.strip()
                # Handle different position formats
                try:
                    # Try to extract just the number part
                    position = int(re.search(r'(\d+)', position_text).group(1))
                except:
                    # Fall back to the entire text
                    try:
                        position = int(position_text)
                    except:
                        position = i + 1  # Default to row index + 1
                
                print(f"Position: {position}")
                
                # Extract title and artist
                # This varies by page structure - try different approaches
                title = ""
                artist = ""
                
                # Typically the title/artist cell is the third cell
                if len(cells) >= 3:
                    title_artist_cell = cells[2]
                    cell_text = title_artist_cell.text.strip()
                    
                    # Try to find explicit title and artist elements
                    try:
                        # Look for title element - various possible selectors
                        title_selectors = [
                            "span[class*='Title']", 
                            "div[class*='Title']",
                            "span[class*='title']",
                            "div[class*='title']"
                        ]
                        
                        for selector in title_selectors:
                            try:
                                title_elem = title_artist_cell.find_element(By.CSS_SELECTOR, selector)
                                title = title_elem.text.strip()
                                if title:
                                    break
                            except:
                                continue
                        
                        # Look for artist element - various possible selectors
                        artist_selectors = [
                            "span[class*='Artist']", 
                            "div[class*='Artist']",
                            "a[class*='artist']",
                            "div[class*='artist']",
                            "p"  # Often in a paragraph tag
                        ]
                        
                        for selector in artist_selectors:
                            try:
                                artist_elems = title_artist_cell.find_elements(By.CSS_SELECTOR, selector)
                                if artist_elems:
                                    # If multiple artist elements, join them
                                    artist = ", ".join([a.text.strip() for a in artist_elems if a.text.strip()])
                                    if artist:
                                        break
                            except:
                                continue
                        
                    except Exception as e:
                        print(f"Error extracting title/artist elements: {e}")
                    
                    # If we couldn't find with specific selectors, try splitting the cell text
                    if not title or not artist:
                        lines = cell_text.split('\n')
                        if len(lines) >= 2:
                            title = lines[0].strip()
                            artist = lines[1].strip()
                        elif len(lines) == 1:
                            # If only one line, treat it as title
                            title = lines[0].strip()
                            artist = ""
                
                # Another approach: Look for links which are often artist names
                if not artist:
                    try:
                        artist_links = title_artist_cell.find_elements(By.TAG_NAME, "a")
                        if artist_links:
                            artist = ", ".join([a.text.strip() for a in artist_links if a.text.strip()])
                    except:
                        pass
                
                print(f"Title: '{title}', Artist: '{artist}'")
                
                # Extract additional metrics from remaining cells
                peak = ""
                prev = ""
                streak = ""
                streams = ""
                
                # Different chart layouts have different cell indexes for these metrics
                if len(cells) >= 4:
                    try:
                        peak = cells[3].text.strip()
                    except:
                        pass
                
                if len(cells) >= 5:
                    try:
                        prev = cells[4].text.strip()
                    except:
                        pass
                
                if len(cells) >= 6:
                    try:
                        streak = cells[5].text.strip()
                    except:
                        pass
                
                if len(cells) >= 7:
                    try:
                        streams_text = cells[6].text.strip()
                        # Remove commas for numeric storage
                        streams = streams_text.replace(",", "")
                    except:
                        pass
                
                print(f"Metrics: Peak: {peak}, Previous: {prev}, Streak: {streak}, Streams: {streams}")
                
                # Store the data
                entry_data = {
                    "chart_date": display_date,
                    "position": position,
                    "title": title,
                    "artist": artist,
                    "peak": peak,
                    "prev": prev,
                    "streak": streak,
                    "streams": streams
                }
                
                # Only process entries with valid title
                if title:
                    # Add release date from MusicBrainz (with caching)
                    track_key = f"{title}|{artist}"
                    if track_key in musicbrainz_cache:
                        entry_data["release_date"] = musicbrainz_cache[track_key]
                        print(f"Using cached release date for {track_key}: {entry_data['release_date']}")
                    else:
                        entry_data["release_date"] = get_release_date(title, artist)
                        musicbrainz_cache[track_key] = entry_data["release_date"]
                    
                    # Process historical data
                    historical_data = get_historical_data_for_track(db_connection, chart_type, title, artist)
                    if historical_data:
                        entry_data["first_entry_date"] = historical_data.get("first_entry_date", "")
                        entry_data["first_entry_position"] = historical_data.get("first_entry_position", "")
                        entry_data["total_days_on_chart"] = historical_data.get("total_days_on_chart", "")
                    else:
                        # If no historical data, this is the first entry
                        entry_data["first_entry_date"] = display_date
                        entry_data["first_entry_position"] = position
                        entry_data["total_days_on_chart"] = "1"
                    
                    # Save this entry to database immediately
                    save_chart_entry_to_db(db_connection, chart_type, entry_data)
                    
                    chart_data.append(entry_data)
                    print(f"✓ Entry #{i+1} processed and saved successfully")
                    logging.info(f"Processed entry #{i+1}: {title} by {artist}")
                else:
                    print(f"✗ Entry #{i+1} skipped (no title found)")
                
            except Exception as e:
                print(f"❌ Error parsing row {i+1}: {e}")
                logging.error(f"Error parsing row {i+1}: {e}", exc_info=True)
                continue
        
        print(f"\n✓ Scraped {len(chart_data)} entries from {chart_type} chart for {display_date}")
        logging.info(f"Successfully scraped {len(chart_data)} entries from {chart_type} chart for {display_date}")
        
        # Return the scraped data
        return chart_data
        
    except Exception as e:
        print(f"❌ Error during scraping: {e}")
        logging.error(f"Error during scraping: {e}", exc_info=True)
        return []

def get_release_date_with_crawl4ai(title, artist):
    """
    Get release date information using crawl4ai.
    
    Args:
        title (str): Song title
        artist (str): Artist name
        
    Returns:
        str: Release date in YYYY-MM-DD format or empty string if not found
    """
    try:
        import asyncio
        from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
        
        print(f"Searching for release date of: {title} by {artist}")
        
        # Clean up the title and artist for better search results
        clean_title = re.sub(r'[\(\[].*?[\)\]]', '', title).strip()  # Remove text in parentheses
        clean_artist = artist.split(',')[0].split('&')[0].strip()  # Take first artist if multiple
        
        print(f"Cleaned search terms: title='{clean_title}', artist='{clean_artist}'")
        
        # Create search query for Google
        search_query = f"{clean_title} {clean_artist} song release date"
        search_url = f"https://www.google.com/search?q={search_query}"
        
        # Define async function to run the crawler
        async def run_crawler():
            async with AsyncWebCrawler() as crawler:
                config = CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,  # Don't use cache
                    wait_until='networkidle',  # Wait for network to be idle
                    verbose=True,  # Enable verbose logging
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
                )
                
                # Execute the crawl
                result = await crawler.arun(url=search_url, config=config)
                
                # Check if we got a valid result
                if result and result.markdown:
                    # Look for date patterns in the markdown content
                    markdown_content = result.markdown
                    
                    # Common date formats
                    date_patterns = [
                        r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
                        r'([A-Z][a-z]+ \d{1,2},? \d{4})',  # Month DD, YYYY
                        r'(\d{1,2} [A-Z][a-z]+ \d{4})',  # DD Month YYYY
                        r'Released on (\d{1,2} [A-Z][a-z]+ \d{4})',  # Released on DD Month YYYY
                        r'Released: (\d{1,2} [A-Z][a-z]+ \d{4})',  # Released: DD Month YYYY
                        r'Release date: ([A-Z][a-z]+ \d{1,2},? \d{4})',  # Release date: Month DD, YYYY
                        r'Released in (\d{4})'  # Released in YYYY
                    ]
                    
                    for pattern in date_patterns:
                        matches = re.findall(pattern, markdown_content)
                        if matches:
                            date_str = matches[0]
                            print(f"Found potential release date: {date_str}")
                            
                            # Try to convert to YYYY-MM-DD format
                            try:
                                # Try different date formats
                                for fmt in ['%Y-%m-%d', '%B %d, %Y', '%B %d %Y', '%d %B %Y']:
                                    try:
                                        date_obj = datetime.datetime.strptime(date_str, fmt)
                                        formatted_date = date_obj.strftime('%Y-%m-%d')
                                        print(f"Converted to: {formatted_date}")
                                        return formatted_date
                                    except ValueError:
                                        continue
                                
                                # If we couldn't parse but found a year
                                year_match = re.search(r'(\d{4})', date_str)
                                if year_match:
                                    year = year_match.group(1)
                                    formatted_date = f"{year}-01-01"
                                    print(f"Using year only: {formatted_date}")
                                    return formatted_date
                            except Exception as e:
                                print(f"Error parsing date: {e}")
                
                # If we couldn't find a release date, try Wikipedia
                wiki_url = f"https://en.wikipedia.org/wiki/{clean_title.replace(' ', '_')}_({clean_artist.replace(' ', '_')}_song)"
                try:
                    wiki_result = await crawler.arun(url=wiki_url, config=config)
                    if wiki_result and wiki_result.markdown:
                        wiki_content = wiki_result.markdown
                        
                        # Look for release date in Wikipedia content
                        for pattern in date_patterns:
                            matches = re.findall(pattern, wiki_content)
                            if matches:
                                date_str = matches[0]
                                print(f"Found potential release date on Wikipedia: {date_str}")
                                
                                # Try to convert to YYYY-MM-DD format
                                try:
                                    # Try different date formats
                                    for fmt in ['%Y-%m-%d', '%B %d, %Y', '%B %d %Y', '%d %B %Y']:
                                        try:
                                            date_obj = datetime.datetime.strptime(date_str, fmt)
                                            formatted_date = date_obj.strftime('%Y-%m-%d')
                                            print(f"Converted to: {formatted_date}")
                                            return formatted_date
                                        except ValueError:
                                            continue
                                    
                                    # If we couldn't parse but found a year
                                    year_match = re.search(r'(\d{4})', date_str)
                                    if year_match:
                                        year = year_match.group(1)
                                        formatted_date = f"{year}-01-01"
                                        print(f"Using year only: {formatted_date}")
                                        return formatted_date
                                except Exception as e:
                                    print(f"Error parsing Wikipedia date: {e}")
                except Exception as e:
                    print(f"Error with Wikipedia search: {e}")
                
                return ""
        
        # Run the async function
        return asyncio.run(run_crawler())
    
    except Exception as e:
        print(f"❌ Error getting release date with crawl4ai: {e}")
        logging.error(f"Error getting release date with crawl4ai: {e}", exc_info=True)
        return ""

def get_release_date(title, artist):
    """
    Get release date using crawl4ai, with caching and improved debugging.
    
    Args:
        title (str): Song title
        artist (str): Artist name
        
    Returns:
        str: Release date in YYYY-MM-DD format or empty string if not found
    """
    print(f"\nGetting release date for: {title} by {artist}")
    
    # Generate cache key
    track_key = f"{title}|{artist}"
    
    # Check combined cache first
    if track_key in musicbrainz_cache and musicbrainz_cache[track_key]:
        print(f"Using cached release date: {musicbrainz_cache[track_key]}")
        return musicbrainz_cache[track_key]
    
    # Try crawl4ai
    print("\nTrying crawl4ai...")
    try:
        release_date = get_release_date_with_crawl4ai(title, artist)
        if release_date:
            print(f"✓ Found release date with crawl4ai: {release_date}")
            
            # Cache the successful result
            musicbrainz_cache[track_key] = release_date
            return release_date
        else:
            print(f"✗ No release date found with crawl4ai")
    except Exception as e:
        print(f"✗ Error with crawl4ai: {e}")
        logging.error(f"Error with crawl4ai: {e}", exc_info=True)
    
    # If crawl4ai failed, try a simpler approach - extract year from title or artist context
    print("\nTrying to extract year from title or artist context...")
    year_match = re.search(r'(19\d{2}|20\d{2})', f"{title} {artist}")
    if year_match:
        year = year_match.group(1)
        print(f"Extracted year from context: {year}")
        release_date = f"{year}-01-01"
        
        # Cache this result too
        musicbrainz_cache[track_key] = release_date
        return release_date
    
    # If everything failed
    print("✗ Could not determine release date from any source")
    musicbrainz_cache[track_key] = ""
    return ""

def setup_database():
    """
    Set up the database with separate tables for global and USA charts.
    
    Returns:
        sqlite3.Connection: Database connection
    """
    try:
        print("\nSetting up database...")
        conn = sqlite3.connect("spotify_charts.db")
        c = conn.cursor()
        
        # Create table for global charts with additional engineered features
        c.execute("""
            CREATE TABLE IF NOT EXISTS global_chart_entries (
                chart_date TEXT,
                position INTEGER,
                title TEXT,
                artist TEXT,
                peak TEXT,
                prev TEXT,
                streak TEXT,
                streams TEXT,
                release_date TEXT,
                first_entry_date TEXT,
                first_entry_position TEXT,
                total_days_on_chart TEXT,
                
                -- Lag features
                prev_streams TEXT,
                prev_position TEXT,
                
                -- Rolling averages
                rolling_avg_streams_3day TEXT,
                rolling_avg_position_3day TEXT,
                
                -- Momentum features
                streams_day_over_day_pct TEXT,
                position_change TEXT,
                
                -- Time-based features
                days_since_release INTEGER,
                
                -- Entry flags
                is_new_entry INTEGER,
                
                -- Calendar features
                is_weekend INTEGER,
                is_holiday INTEGER,
                
                PRIMARY KEY (chart_date, position)
            )
        """)
        
        # Create table for USA charts with additional engineered features
        c.execute("""
            CREATE TABLE IF NOT EXISTS usa_chart_entries (
                chart_date TEXT,
                position INTEGER,
                title TEXT,
                artist TEXT,
                peak TEXT,
                prev TEXT,
                streak TEXT,
                streams TEXT,
                release_date TEXT,
                first_entry_date TEXT,
                first_entry_position TEXT,
                total_days_on_chart TEXT,
                
                -- Lag features
                prev_streams TEXT,
                prev_position TEXT,
                
                -- Rolling averages
                rolling_avg_streams_3day TEXT,
                rolling_avg_position_3day TEXT,
                
                -- Momentum features
                streams_day_over_day_pct TEXT,
                position_change TEXT,
                
                -- Time-based features
                days_since_release INTEGER,
                
                -- Entry flags
                is_new_entry INTEGER,
                
                -- Calendar features
                is_weekend INTEGER,
                is_holiday INTEGER,
                
                PRIMARY KEY (chart_date, position)
            )
        """)
        
        conn.commit()
        print("Database setup complete.")
        return conn
        
    except Exception as e:
        print(f"❌ Error setting up database: {e}")
        logging.error(f"Error setting up database: {e}", exc_info=True)
        raise

def engineer_features(conn, chart_type, entry_data):
    """
    Engineer historical and predictive features for a chart entry.
    
    Args:
        conn (sqlite3.Connection): Database connection
        chart_type (str): Chart type ('global' or 'usa')
        entry_data (dict): Chart entry data
        
    Returns:
        dict: Entry data with additional engineered features
    """
    try:
        c = conn.cursor()
        title = entry_data['title']
        artist = entry_data['artist']
        chart_date = entry_data['chart_date']
        
        # Determine which table to use
        table_name = f"{chart_type}_chart_entries"
        
        # Initialize engineered features with default values
        engineered_data = {
            'prev_streams': '',
            'prev_position': '',
            'rolling_avg_streams_3day': '',
            'rolling_avg_position_3day': '',
            'streams_day_over_day_pct': '',
            'position_change': '',
            'days_since_release': -1,
            'is_new_entry': 0,
            'is_weekend': 0,
            'is_holiday': 0
        }
        
        # 1. Calculate days since release
        if entry_data.get('release_date'):
            try:
                release_date = datetime.datetime.strptime(entry_data['release_date'], '%Y-%m-%d')
                current_date = datetime.datetime.strptime(chart_date, '%Y-%m-%d')
                days_since = (current_date - release_date).days
                engineered_data['days_since_release'] = max(0, days_since)  # Ensure non-negative
                print(f"Days since release for {title}: {engineered_data['days_since_release']}")
            except Exception as e:
                print(f"Error calculating days since release: {e}")
                engineered_data['days_since_release'] = 0  # Default to 0 if calculation fails
        
        # 2. Check if this is a new entry (first appearance on chart)
        if entry_data.get('first_entry_date') == chart_date:
            engineered_data['is_new_entry'] = 1
            print(f"New entry detected for {title}")
        
        # 3. Check if the chart date is a weekend
        try:
            chart_date_obj = datetime.datetime.strptime(chart_date, '%Y-%m-%d')
            if chart_date_obj.weekday() >= 5:  # 5=Saturday, 6=Sunday
                engineered_data['is_weekend'] = 1
                print(f"Weekend detected for date {chart_date}")
        except Exception as e:
            print(f"Error determining weekend: {e}")
        
        # 4. Check if the chart date is a holiday (simplified approach)
        # This is a simplified approach - for production, use a proper holiday calendar library
        holidays = [
            '2024-12-25',  # Christmas
            '2024-12-31',  # New Year's Eve
            '2025-01-01',  # New Year's Day
            '2025-07-04',  # Independence Day (US)
            '2025-12-25',  # Christmas
            '2025-12-31',  # New Year's Eve
            '2026-01-01',  # New Year's Day
        ]
        if chart_date in holidays:
            engineered_data['is_holiday'] = 1
            print(f"Holiday detected for date {chart_date}")
        
        # 5. Get historical data for this track to calculate lag features
        # First, get all previous entries for this track, ordered by date
        c.execute(f"""
            SELECT chart_date, position, streams 
            FROM {table_name} 
            WHERE title = ? AND artist = ? AND chart_date < ?
            ORDER BY chart_date DESC
        """, (title, artist, chart_date))
        
        history = c.fetchall()
        print(f"Found {len(history)} historical entries for {title} by {artist}")
        
        # If we have historical data
        if history:
            # Get the most recent previous entry (should be the first since we ordered DESC)
            prev_date, prev_position, prev_streams = history[0]
            
            # Set previous streams and position
            engineered_data['prev_streams'] = prev_streams
            engineered_data['prev_position'] = prev_position
            print(f"Previous position for {title}: {prev_position}, Previous streams: {prev_streams}")
            
            # Calculate position change
            try:
                current_position = int(entry_data['position'])
                prev_position_int = int(prev_position)
                # Positive means improved position (moved up the chart)
                # e.g., from position 5 to position 3 would be +2
                position_change = prev_position_int - current_position
                engineered_data['position_change'] = str(position_change)
                print(f"Position change for {title}: {position_change}")
            except Exception as e:
                print(f"Error calculating position change: {e}")
            
            # Calculate day-over-day percentage change in streams
            try:
                # Make sure to handle commas in stream counts
                current_streams = int(entry_data['streams'].replace(',', ''))
                prev_streams = int(prev_streams.replace(',', '') if isinstance(prev_streams, str) else prev_streams)
                
                if prev_streams > 0:
                    pct_change = ((current_streams - prev_streams) / prev_streams) * 100
                    engineered_data['streams_day_over_day_pct'] = f"{pct_change:.2f}"
                    print(f"Stream change % for {title}: {pct_change:.2f}%")
            except Exception as e:
                print(f"Error calculating stream percentage change: {e}")
        
        # 6. Calculate rolling averages (3-day)
        # Need at least 2 previous entries + current for 3-day average
        if len(history) >= 2:
            try:
                # Start with current entry's streams and position
                stream_values = []
                position_values = []
                
                # Add current entry
                try:
                    current_streams = int(entry_data['streams'].replace(',', ''))
                    current_position = int(entry_data['position'])
                    stream_values.append(current_streams)
                    position_values.append(current_position)
                    
                    # Add up to 2 previous entries
                    count = 0
                    for _, h_position, h_streams in history:
                        if count < 2:  # Get up to 2 previous entries
                            h_streams_int = int(h_streams.replace(',', '') if isinstance(h_streams, str) else h_streams)
                            h_position_int = int(h_position)
                            
                            stream_values.append(h_streams_int)
                            position_values.append(h_position_int)
                            count += 1
                    
                    # Calculate averages
                    if len(stream_values) > 1:
                        avg_streams = sum(stream_values) / len(stream_values)
                        avg_position = sum(position_values) / len(position_values)
                        engineered_data['rolling_avg_streams_3day'] = f"{avg_streams:.2f}"
                        engineered_data['rolling_avg_position_3day'] = f"{avg_position:.2f}"
                        print(f"3-day rolling avg for {title}: Streams={avg_streams:.2f}, Position={avg_position:.2f}")
                except Exception as e:
                    print(f"Error in current entry processing for rolling average: {e}")
            except Exception as e:
                print(f"Error calculating rolling averages: {e}")
        
        # Merge engineered features with original entry data
        entry_data.update(engineered_data)
        return entry_data
        
    except Exception as e:
        print(f"Error engineering features: {e}")
        logging.error(f"Error engineering features: {e}", exc_info=True)
        return entry_data  # Return original data if engineering fails

def save_chart_entry_to_db(conn, chart_type, entry_data):
    """
    Save a single chart entry to the appropriate database table.
    
    Args:
        conn (sqlite3.Connection): Database connection
        chart_type (str): Chart type ('global' or 'usa')
        entry_data (dict): Chart entry data
    """
    try:
        c = conn.cursor()
        
        # Engineer additional features
        entry_data = engineer_features(conn, chart_type, entry_data)
        
        # Determine which table to use
        table_name = f"{chart_type}_chart_entries"
        
        # Insert the entry with engineered features
        c.execute(f"""
            INSERT OR REPLACE INTO {table_name} (
                chart_date, position, title, artist, peak, prev, streak, streams,
                release_date, first_entry_date, first_entry_position, total_days_on_chart,
                prev_streams, prev_position, rolling_avg_streams_3day, rolling_avg_position_3day,
                streams_day_over_day_pct, position_change, days_since_release, is_new_entry,
                is_weekend, is_holiday
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                entry_data['chart_date'], entry_data['position'], entry_data['title'], 
                entry_data['artist'], entry_data.get('peak', ''), entry_data.get('prev', ''), 
                entry_data.get('streak', ''), entry_data.get('streams', ''), 
                entry_data.get('release_date', ''), entry_data.get('first_entry_date', ''), 
                entry_data.get('first_entry_position', ''), entry_data.get('total_days_on_chart', ''),
                entry_data.get('prev_streams', ''), entry_data.get('prev_position', ''),
                entry_data.get('rolling_avg_streams_3day', ''), entry_data.get('rolling_avg_position_3day', ''),
                entry_data.get('streams_day_over_day_pct', ''), entry_data.get('position_change', ''),
                entry_data.get('days_since_release', -1), entry_data.get('is_new_entry', 0),
                entry_data.get('is_weekend', 0), entry_data.get('is_holiday', 0)
            ))
        
        conn.commit()
        
    except Exception as e:
        print(f"❌ Error saving entry to database: {e}")
        logging.error(f"Error saving entry to database: {e}", exc_info=True)

def get_historical_data_for_track(conn, chart_type, title, artist):
    """
    Get historical chart data for a specific track.
    
    Args:
        conn (sqlite3.Connection): Database connection
        chart_type (str): Chart type ('global' or 'usa')
        title (str): Track title
        artist (str): Artist name
        
    Returns:
        dict: Historical data including first entry date, position, and total days
    """
    try:
        c = conn.cursor()
        
        # Determine which table to use
        table_name = f"{chart_type}_chart_entries"
        
        # Query for all entries of this track
        c.execute(f"SELECT chart_date, position FROM {table_name} WHERE title = ? AND artist = ? ORDER BY chart_date", 
                 (title, artist))
        
        entries = c.fetchall()
        
        if entries:
            # First entry is the earliest date
            first_entry_date, first_entry_position = entries[0]
            
            # Total days is the count of entries
            total_days = len(entries)
            
            return {
                "first_entry_date": first_entry_date,
                "first_entry_position": first_entry_position,
                "total_days_on_chart": str(total_days)
            }
        
        return None
        
    except Exception as e:
        print(f"❌ Error getting historical data: {e}")
        logging.error(f"Error getting historical data: {e}", exc_info=True)
        return None

def process_chart_data(conn, chart_type, chart_date):
    """
    Process all chart entries for a specific date to update engineered features.
    This function should be called after all entries for a date have been saved to the database.
    
    Args:
        conn (sqlite3.Connection): Database connection
        chart_type (str): Chart type ('global' or 'usa')
        chart_date (str): The chart date to process
    """
    try:
        c = conn.cursor()
        table_name = f"{chart_type}_chart_entries"
        
        print(f"\nProcessing engineered features for {chart_type} chart on {chart_date}...")
        
        # First, get all chart dates in the database, ordered by date
        c.execute(f"SELECT DISTINCT chart_date FROM {table_name} ORDER BY chart_date")
        all_dates = [date[0] for date in c.fetchall()]
        
        if not all_dates:
            print("No dates found in database")
            return
            
        print(f"Found {len(all_dates)} dates in database: {all_dates}")
        
        # Find the index of the current date
        if chart_date not in all_dates:
            print(f"Current date {chart_date} not found in database dates")
            return
            
        current_date_index = all_dates.index(chart_date)
        
        # Get all entries for this chart date
        c.execute(f"SELECT title, artist, position, streams FROM {table_name} WHERE chart_date = ?", (chart_date,))
        current_entries = c.fetchall()
        
        print(f"Processing {len(current_entries)} entries for {chart_date}")
        
        for title, artist, position, streams in current_entries:
            # Initialize engineered features
            engineered_data = {}
            
            # 1. Calculate days since release
            c.execute(f"SELECT release_date FROM {table_name} WHERE title = ? AND artist = ? AND chart_date = ? LIMIT 1", 
                     (title, artist, chart_date))
            release_date_row = c.fetchone()
            
            if release_date_row and release_date_row[0]:
                try:
                    release_date = datetime.datetime.strptime(release_date_row[0], '%Y-%m-%d')
                    current_date_obj = datetime.datetime.strptime(chart_date, '%Y-%m-%d')
                    days_since = (current_date_obj - release_date).days
                    engineered_data['days_since_release'] = max(0, days_since)  # Ensure non-negative
                    print(f"Days since release for {title}: {engineered_data['days_since_release']}")
                except Exception as e:
                    print(f"Error calculating days since release: {e}")
                    engineered_data['days_since_release'] = 0
            
            # 2. Check if the chart date is a weekend
            try:
                chart_date_obj = datetime.datetime.strptime(chart_date, '%Y-%m-%d')
                if chart_date_obj.weekday() >= 5:  # 5=Saturday, 6=Sunday
                    engineered_data['is_weekend'] = 1
                    print(f"Weekend detected for {title} on {chart_date}")
                else:
                    engineered_data['is_weekend'] = 0
            except Exception as e:
                print(f"Error determining weekend: {e}")
                engineered_data['is_weekend'] = 0
            
            # 3. Check if the chart date is a holiday (simplified approach)
            holidays = [
                '2024-12-25',  # Christmas
                '2024-12-31',  # New Year's Eve
                '2025-01-01',  # New Year's Day
                '2025-07-04',  # Independence Day (US)
                '2025-12-25',  # Christmas
                '2025-12-31',  # New Year's Eve
                '2026-01-01',  # New Year's Day
            ]
            if chart_date in holidays:
                engineered_data['is_holiday'] = 1
                print(f"Holiday detected for {title} on {chart_date}")
            else:
                engineered_data['is_holiday'] = 0
            
            # 4. Check if this is a new entry (first appearance on chart)
            # Look for any previous appearances of this song
            c.execute(f"""
                SELECT COUNT(*) FROM {table_name} 
                WHERE title = ? AND artist = ? AND chart_date < ?
            """, (title, artist, chart_date))
            
            previous_appearances = c.fetchone()[0]
            if previous_appearances == 0:
                engineered_data['is_new_entry'] = 1
                print(f"New entry detected for {title}")
                
                # This is the first entry, so set first_entry_date and first_entry_position
                engineered_data['first_entry_date'] = chart_date
                engineered_data['first_entry_position'] = position
                engineered_data['total_days_on_chart'] = "1"
            else:
                engineered_data['is_new_entry'] = 0
                
                # Get the earliest entry for this song
                c.execute(f"""
                    SELECT chart_date, position FROM {table_name} 
                    WHERE title = ? AND artist = ? 
                    ORDER BY chart_date ASC LIMIT 1
                """, (title, artist))
                
                first_entry = c.fetchone()
                if first_entry:
                    engineered_data['first_entry_date'] = first_entry[0]
                    engineered_data['first_entry_position'] = first_entry[1]
                    
                    # Calculate total days on chart
                    c.execute(f"""
                        SELECT COUNT(DISTINCT chart_date) FROM {table_name} 
                        WHERE title = ? AND artist = ? AND chart_date <= ?
                    """, (title, artist, chart_date))
                    
                    total_days = c.fetchone()[0]
                    engineered_data['total_days_on_chart'] = str(total_days)
            
            # 5. Get previous day's data for this track if available
            if current_date_index > 0:
                previous_date = all_dates[current_date_index - 1]
                
                c.execute(f"""
                    SELECT position, streams FROM {table_name} 
                    WHERE title = ? AND artist = ? AND chart_date = ?
                """, (title, artist, previous_date))
                
                prev_entry = c.fetchone()
                if prev_entry:
                    prev_position, prev_streams = prev_entry
                    
                    # Set previous position and streams
                    engineered_data['prev_position'] = prev_position
                    engineered_data['prev_streams'] = prev_streams
                    print(f"Previous position for {title}: {prev_position}, Previous streams: {prev_streams}")
                    
                    # Calculate position change
                    try:
                        current_position = int(position)
                        prev_position_int = int(prev_position)
                        # Positive means improved position (moved up the chart)
                        # e.g., from position 5 to position 3 would be +2
                        position_change = prev_position_int - current_position
                        engineered_data['position_change'] = str(position_change)
                        print(f"Position change for {title}: {position_change}")
                    except Exception as e:
                        print(f"Error calculating position change: {e}")
                    
                    # Calculate day-over-day percentage change in streams
                    try:
                        # Make sure to handle commas in stream counts
                        current_streams = int(streams.replace(',', '') if isinstance(streams, str) else streams)
                        prev_streams_int = int(prev_streams.replace(',', '') if isinstance(prev_streams, str) else prev_streams)
                        
                        if prev_streams_int > 0:
                            pct_change = ((current_streams - prev_streams_int) / prev_streams_int) * 100
                            engineered_data['streams_day_over_day_pct'] = f"{pct_change:.2f}"
                            print(f"Stream change % for {title}: {pct_change:.2f}%")
                    except Exception as e:
                        print(f"Error calculating stream percentage change: {e}")
            
            # 6. Calculate rolling averages (3-day)
            # Need current day plus up to 2 previous days
            rolling_window = 3
            stream_values = []
            position_values = []
            
            # Add current entry
            try:
                current_streams = int(streams.replace(',', '') if isinstance(streams, str) else streams)
                current_position_int = int(position)
                stream_values.append(current_streams)
                position_values.append(current_position_int)
                
                # Add previous days' data
                for i in range(1, rolling_window):
                    if current_date_index - i >= 0:
                        prev_date = all_dates[current_date_index - i]
                        
                        c.execute(f"""
                            SELECT position, streams FROM {table_name} 
                            WHERE title = ? AND artist = ? AND chart_date = ?
                        """, (title, artist, prev_date))
                        
                        prev_entry = c.fetchone()
                        if prev_entry:
                            prev_pos, prev_str = prev_entry
                            try:
                                prev_streams_int = int(prev_str.replace(',', '') if isinstance(prev_str, str) else prev_str)
                                prev_position_int = int(prev_pos)
                                stream_values.append(prev_streams_int)
                                position_values.append(prev_position_int)
                            except Exception as e:
                                print(f"Error converting previous values: {e}")
                
                # Calculate averages if we have enough data
                if len(stream_values) > 1:
                    avg_streams = sum(stream_values) / len(stream_values)
                    avg_position = sum(position_values) / len(position_values)
                    engineered_data['rolling_avg_streams_3day'] = f"{avg_streams:.2f}"
                    engineered_data['rolling_avg_position_3day'] = f"{avg_position:.2f}"
                    print(f"Rolling avg for {title} ({len(stream_values)} days): Streams={avg_streams:.2f}, Position={avg_position:.2f}")
            except Exception as e:
                print(f"Error calculating rolling averages: {e}")
            
            # Update the entry with the engineered features
            if engineered_data:
                update_query = f"UPDATE {table_name} SET "
                update_parts = []
                params = []
                
                for key, value in engineered_data.items():
                    update_parts.append(f"{key} = ?")
                    params.append(value)
                
                update_query += ", ".join(update_parts)
                update_query += " WHERE chart_date = ? AND title = ? AND artist = ?"
                params.extend([chart_date, title, artist])
                
                c.execute(update_query, params)
                conn.commit()
                print(f"Updated engineered features for {title} by {artist} on {chart_date}")
            
        print(f"Finished processing engineered features for {chart_type} chart on {chart_date}")
    
    except Exception as e:
        print(f"Error processing chart data: {e}")
        logging.error(f"Error processing chart data: {e}", exc_info=True)

def get_latest_chart_date(driver):
    """
    Extract the actual date from the current chart page.
    
    Args:
        driver (webdriver.Chrome): The Selenium WebDriver instance
        
    Returns:
        str: The actual date of the chart in YYYY-MM-DD format
    """
    print("Extracting chart date from current page...")
    
    # Wait for page to fully load
    time.sleep(5)
    
    # Take screenshot for debugging
    driver.save_screenshot("chart_page.png")
    print("Saved screenshot as chart_page.png for debugging")
    
    # Get page source for debugging
    page_source = driver.page_source
    with open("page_source.html", "w", encoding="utf-8") as f:
        f.write(page_source)
    print("Saved page source as page_source.html for debugging")
    
    # 1. First try: Look for the main heading that typically contains the date
    print("Strategy 1: Looking for main heading with date...")
    main_headings = driver.find_elements(By.CSS_SELECTOR, "h1, h2")
    for heading in main_headings:
        heading_text = heading.text.strip()
        print(f"Found heading: '{heading_text}'")
        
        # Look for date pattern after the dot/bullet
        date_match = re.search(r'·\s*([A-Za-z]+\s+\d+,?\s*\d{4})', heading_text)
        if date_match:
            date_str = date_match.group(1).strip()
            print(f"Found date in heading: '{date_str}'")
            
            # Try parsing with different formats
            for fmt in ['%B %d, %Y', '%B %d %Y', '%b %d, %Y', '%b %d %Y']:
                try:
                    date_obj = datetime.datetime.strptime(date_str, fmt)
                    formatted_date = date_obj.strftime('%Y-%m-%d')
                    print(f"Successfully parsed chart date: {formatted_date}")
                    return formatted_date
                except ValueError:
                    continue
    
    # 2. Second try: Look for any element with "chart" and a date nearby
    print("Strategy 2: Looking for elements with 'chart' and date...")
    chart_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'chart') or contains(text(), 'Chart') or contains(text(), 'Top')]")
    for element in chart_elements:
        element_text = element.text.strip()
        print(f"Found chart-related element: '{element_text}'")
        
        # Look for date patterns in various formats
        date_patterns = [
            r'([A-Za-z]+\s+\d+,?\s*\d{4})',  # May 18, 2025 or May 18 2025
            r'(\d{1,2}/\d{1,2}/\d{4})',      # 5/18/2025
            r'(\d{4}-\d{2}-\d{2})'           # 2025-05-18
        ]
        
        for pattern in date_patterns:
            date_match = re.search(pattern, element_text)
            if date_match:
                date_str = date_match.group(1).strip()
                print(f"Found date: '{date_str}'")
                
                # Try parsing with different formats
                for fmt in ['%B %d, %Y', '%B %d %Y', '%b %d, %Y', '%b %d %Y', '%m/%d/%Y', '%Y-%m-%d']:
                    try:
                        date_obj = datetime.datetime.strptime(date_str, fmt)
                        formatted_date = date_obj.strftime('%Y-%m-%d')
                        print(f"Successfully parsed chart date: {formatted_date}")
                        return formatted_date
                    except ValueError:
                        continue
    
    # 3. Third try: Check URL for date
    print("Strategy 3: Checking URL for date...")
    current_url = driver.current_url
    print(f"Current URL: {current_url}")
    
    url_date_match = re.search(r'/(\d{4}-\d{2}-\d{2})$', current_url)
    if url_date_match:
        url_date = url_date_match.group(1)
        print(f"Found date in URL: {url_date}")
        return url_date
    
    # 4. Fourth try: Look for any date-like text on the page
    print("Strategy 4: Looking for any date-like text on page...")
    # Get all visible text on the page
    body_text = driver.find_element(By.TAG_NAME, "body").text
    
    # Look for various date formats in the entire page text
    all_dates = re.findall(r'([A-Za-z]+\s+\d{1,2},?\s*\d{4})', body_text)
    if all_dates:
        for date_str in all_dates:
            print(f"Found potential date in page text: '{date_str}'")
            for fmt in ['%B %d, %Y', '%B %d %Y', '%b %d, %Y', '%b %d %Y']:
                try:
                    date_obj = datetime.datetime.strptime(date_str, fmt)
                    formatted_date = date_obj.strftime('%Y-%m-%d')
                    print(f"Successfully parsed chart date: {formatted_date}")
                    return formatted_date
                except ValueError:
                    continue
    
    # If we get here, we've failed to find a date
    raise Exception("Failed to extract chart date from page. Check screenshot and page source for debugging.")

def generate_date_range(start_date, days=90):
    """
    Generate a list of dates going back a specified number of days from a given start date.
    
    Args:
        start_date (str): The start date in YYYY-MM-DD format
        days (int): Number of days to go back
        
    Returns:
        list: List of dates in YYYY-MM-DD format
    """
    try:
        # Convert start_date string to datetime object
        start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        date_list = []
        
        # Generate dates going back
        for i in range(days):
            date = start - datetime.timedelta(days=i)
            date_list.append(date.strftime('%Y-%m-%d'))
        
        return date_list
    except Exception as e:
        print(f"Error generating date range: {e}")
        logging.error(f"Error generating date range: {e}", exc_info=True)
        return []

def get_chart_types():
    """Return predefined chart types to scrape."""
    return ['global', 'usa']  # Both chart types

def main():
    try:
        print("\n" + "="*50)
        print("SPOTIFY CHARTS SCRAPER - IMPROVED VERSION".center(50))
        print("="*50)
        
        # Use both chart types by default
        chart_types = get_chart_types()
        print(f"Will scrape charts for: {', '.join(t.upper() for t in chart_types)}")
        
        # Set up database first
        conn = setup_database()
        
        driver = setup_driver()
        try:
            print("\n" + "-"*50)
            print("AUTHENTICATION".center(50))
            print("-"*50)
            login_to_spotify(driver, EMAIL, PASSWORD)
            
            print("\n" + "-"*50)
            print("DATA COLLECTION".center(50))
            print("-"*50)
            
            for chart_type in chart_types:
                print(f"\n{'='*20} {chart_type.upper()} CHARTS {'='*20}")
                
                # Build the base URL based on chart type
                if chart_type == 'global':
                    base_url = "https://charts.spotify.com/charts/view/regional-global-daily"
                else:  # usa
                    base_url = "https://charts.spotify.com/charts/view/regional-us-daily"
                
                # First navigate to the latest chart URL
                latest_url = f"{base_url}/latest"
                print(f"Navigating to latest chart URL: {latest_url}")
                driver.get(latest_url)
                
                # Wait for page to load properly
                time.sleep(5)
                
                try:
                    # Extract the actual date from the current page
                    latest_date = get_latest_chart_date(driver)
                    print(f"Latest chart date for {chart_type}: {latest_date}")
                    
                    # Generate date range starting from the actual latest date
                    date_range = generate_date_range(latest_date, 90)  # 90 days including the latest
                    print(f"Will scrape charts for dates: {', '.join(date_range)}")
                    
                    # Process each date in our range
                    for chart_date in date_range:
                        print(f"\n{'-'*20} Date: {chart_date} {'-'*20}")
                        
                        url = f"{base_url}/{chart_date}"
                        print(f"URL: {url}")
                        
                        try:
                            # Scrape the chart data - this now saves to DB incrementally
                            data = scrape_spotify_charts(driver, url, chart_date, chart_type, conn)
                            
                            if data:
                                print(f"✓ Successfully scraped and saved {len(data)} entries")
                                
                                # Process engineered features for this date after all entries are saved
                                process_chart_data(conn, chart_type, chart_date)
                            else:
                                print("No data scraped for this date")
                        except Exception as e:
                            print(f"❌ Failed to scrape {chart_type} chart for {chart_date}: {e}")
                            logging.error(f"Failed to scrape {chart_type} chart for {chart_date}: {e}")
                            continue
                
                except Exception as e:
                    print(f"❌ Critical error: Failed to determine latest chart date for {chart_type}: {e}")
                    logging.error(f"Failed to determine latest chart date for {chart_type}: {e}", exc_info=True)
                    print("Check chart_page.png and page_source.html for debugging")
                    continue
            
            print("\n" + "-"*50)
            print("SUMMARY".center(50))
            print("-"*50)
            
            # Get count of entries in each table
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM global_chart_entries")
            global_count = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM usa_chart_entries")
            usa_count = c.fetchone()[0]
            
            print(f"Total entries collected: {global_count + usa_count}")
            print(f"Global chart entries: {global_count}")
            print(f"USA chart entries: {usa_count}")
            print(f"Data has been saved to spotify_charts.db")
            
            # Close the database connection
            conn.close()
            
        except Exception as e:
            print(f"\n❌ Script failed: {e}")
            logging.error(f"Script failed: {e}")
        finally:
            print("\nClosing WebDriver...")
            driver.quit()
            print("WebDriver closed.")
            print("\n" + "="*50)
            print("SCRAPING COMPLETE".center(50))
            print("="*50)
    
    except KeyboardInterrupt:
        print("\n\nScript interrupted by user. Exiting...")
        try:
            driver.quit()
        except:
            pass
        try:
            conn.close()
        except:
            pass

if __name__ == "__main__":
    main()