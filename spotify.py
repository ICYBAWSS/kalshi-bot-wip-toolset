from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import sqlite3
import logging
import os
import datetime
import requests
import json
import re
from tqdm import tqdm

# Configure logging
logging.basicConfig(filename='spotify_scraper.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Login credentials
EMAIL = "spotify765756@gmail.com"  # Your Spotify email
PASSWORD = "Poop12345#"            # Your Spotify password

# MusicBrainz API settings
MUSICBRAINZ_API_BASE = "https://musicbrainz.org/ws/2"
USER_AGENT = "SpotifyChartsScraper/1.0(research project) email: rayhan.mohaed64.com"  # Change to your email

def setup_driver():
    """Set up and return the Chrome WebDriver."""
    print("Setting up Chrome WebDriver...")
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless')  # Uncomment when not debugging
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    print("WebDriver setup complete.")
    return driver

def login_to_spotify(driver, email, password):
    """Login to Spotify using the password flow with robust fallback options."""
    try:
        print("Navigating to Spotify Charts...")
        logging.info("Navigating to Spotify Charts...")
        driver.get("https://charts.spotify.com")

        # Take screenshot of initial page
        #driver.save_screenshot("01_initial_page.png")
        print("Screenshot saved as '01_initial_page.png'")

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
        
        # Take screenshot after clicking continue
        #driver.save_screenshot("04_after_continue.png")
        #print("Screenshot saved as '04_after_continue.png'")

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
                #driver.save_screenshot("password_entry_error.png")
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
            # Take final screenshot
            #driver.save_screenshot("09_login_timeout.png")
            #print("Screenshot saved as '09_login_timeout.png'")
            print("Login timeout or failed. Checking current URL...")
            print(f"Current URL: {driver.current_url}")
            
            if "charts.spotify.com" in driver.current_url:
                print("URL contains charts.spotify.com - assuming login successful despite timeout")
                logging.info("Login successful (detected by URL)")
                return True
            else:
                raise Exception("Login failed - could not reach charts page")
        
    except Exception as e:
        print(f"❌ Login failed: {e}")
        logging.error(f"Login failed: {e}", exc_info=True)
        #driver.save_screenshot("login_error_final.png")
        #print(f"Final error screenshot saved as 'login_error_final.png'")
        raise

def scrape_spotify_charts(driver, url, chart_date, chart_type, db_connection=None):
    """
    Scrape chart data from top 5 entries of Spotify Charts.
    
    Args:
        driver (webdriver.Chrome): The Selenium WebDriver instance.
        url (str): The URL of the Spotify chart.
        chart_date (str): The date of the chart ('latest' or 'YYYY-MM-DD').
        chart_type (str): The type of chart (e.g., 'top200').
        db_connection: Database connection to check for existing entries.

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
        
        # Take a screenshot of the loaded page for debugging
        #driver.save_screenshot(f"loaded_page_{chart_type}.png")
        #print(f"Screenshot saved as 'loaded_page_{chart_type}.png'")
        
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
        
        print(f"\nScraping top 5 entries from {chart_type} chart for {display_date}")
        logging.info(f"Scraping chart for date: {display_date}")
        
        # Get all chart rows
        chart_rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        
        # Only take the top 5 rows
        top_5_rows = chart_rows[:5]
        
        print(f"Found {len(top_5_rows)} rows for top 5 chart entries")
        logging.info(f"Found {len(top_5_rows)} rows for top 5 chart entries")
        
        # Take screenshot of the table for debugging
        #driver.save_screenshot(f"table_{chart_type}_{display_date}.png")
        #print(f"Screenshot saved as 'table_{chart_type}_{display_date}.png'")

        # Process the rows
        chart_data = []
        for i, row in enumerate(top_5_rows):
            print(f"Processing entry #{i+1}...")
            
            # Take screenshot of this row
            driver.execute_script("arguments[0].scrollIntoView();", row)
            time.sleep(0.5)
            #driver.save_screenshot(f"row_{i+1}_{chart_type}.png")
            
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
                    "chart_type": chart_type,
                    "position": position,
                    "title": title,
                    "artist": artist,
                    "peak": peak,
                    "prev": prev,
                    "streak": streak,
                    "streams": streams
                }
                
                # Only add entries with valid title
                if title:
                    chart_data.append(entry_data)
                    print(f"✓ Entry #{i+1} processed successfully")
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
        #driver.save_screenshot(f"error_{chart_type}_{chart_date}.png")
        return []

def get_release_date_from_musicbrainz(title, artist):
    """
    Get release date information from MusicBrainz API.
    
    Args:
        title (str): Song title
        artist (str): Artist name
        
    Returns:
        str: Release date in YYYY-MM-DD format or empty string if not found
    """
    try:
        print(f"Searching MusicBrainz for: {title} by {artist}")
        
        # Use the recording endpoint to search for the track
        search_url = f"{MUSICBRAINZ_API_BASE}/recording"
        
        # Prepare query params - combine title and artist for better results
        params = {
            'query': f'recording:"{title}" AND artist:"{artist}"',
            'fmt': 'json'
        }
        
        headers = {
            'User-Agent': USER_AGENT
        }
        
        # Make the API request
        response = requests.get(search_url, params=params, headers=headers)
        
        # Check if request was successful
        if response.status_code == 200:
            data = response.json()
            
            # Check if we got any recordings
            if 'recordings' in data and len(data['recordings']) > 0:
                # Get the first/best match
                recording = data['recordings'][0]
                
                # Look for release date in the first release
                if 'releases' in recording and len(recording['releases']) > 0:
                    release = recording['releases'][0]
                    
                    # Extract date from release
                    if 'date' in release:
                        release_date = release['date']
                        print(f"Found release date: {release_date}")
                        return release_date
        
        # If we couldn't find a specific release date
        print("No release date found in MusicBrainz")
        return ""
        
    except Exception as e:
        print(f"Error getting release date from MusicBrainz: {e}")
        logging.error(f"Error getting release date from MusicBrainz: {e}", exc_info=True)
        return ""

def process_chart_history(chart_data, historical_data):
    """
    Process chart data with historical information to calculate:
    - first_entry_date
    - first_entry_position
    - total_days_on_chart
    
    Args:
        chart_data (list): List of current chart entries
        historical_data (dict): Dictionary of historical chart data keyed by track_id
        
    Returns:
        list: Updated chart data with additional metrics
    """
    # Create a unique track identifier
    for entry in chart_data:
        # Create a unique identifier for the track (title + artist)
        track_id = f"{entry['title']}|{entry['artist']}"
        
        # Default values
        entry['first_entry_date'] = ""
        entry['first_entry_position'] = ""
        entry['total_days_on_chart'] = ""
        
        # Check if we have historical data for this track
        if track_id in historical_data:
            track_history = historical_data[track_id]
            
            # Sort chart appearances by date
            track_history.sort(key=lambda x: x['chart_date'])
            
            # First entry is the earliest date in our records
            if track_history:
                entry['first_entry_date'] = track_history[0]['chart_date']
                entry['first_entry_position'] = track_history[0]['position']
                entry['total_days_on_chart'] = str(len(track_history))
        
        # Get release date from MusicBrainz
        entry['release_date'] = get_release_date_from_musicbrainz(entry['title'], entry['artist'])
                
    return chart_data

def save_to_database(chart_data):
    """
    Save chart data to SQLite database with updated schema
    
    Args:
        chart_data (list): List of chart entries to save
    """
    try:
        print("\nSaving data to database...")
        conn = sqlite3.connect("spotify_charts.db")
        c = conn.cursor()
        
        # Create table if it doesn't exist
        c.execute("""
            CREATE TABLE IF NOT EXISTS chart_entries (
                chart_date TEXT,
                chart_type TEXT,
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
                PRIMARY KEY (chart_date, chart_type, position)
            )
        """)
        
        # Insert all chart entries
        for entry in chart_data:
            try:
                c.execute("""
                    INSERT OR REPLACE INTO chart_entries (
                        chart_date, chart_type, position, title, artist, peak, prev, streak, streams,
                        release_date, first_entry_date, first_entry_position, total_days_on_chart
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        entry['chart_date'], entry['chart_type'], entry['position'], entry['title'], 
                        entry['artist'], entry.get('peak', ''), entry.get('prev', ''), entry.get('streak', ''), 
                        entry.get('streams', ''), entry.get('release_date', ''), entry.get('first_entry_date', ''), 
                        entry.get('first_entry_position', ''), entry.get('total_days_on_chart', '')
                    ))
                print(f"Saved entry: {entry['title']} by {entry['artist']}")
            except sqlite3.Error as e:
                print(f"Error saving entry: {e}")
                logging.error(f"Database error: {e}")
        
        conn.commit()
        conn.close()
        print(f"✓ Successfully saved {len(chart_data)} entries to database")
        
    except Exception as e:
        print(f"❌ Error saving to database: {e}")
        logging.error(f"Error saving to database: {e}", exc_info=True)

def get_historical_data_from_db():
    """
    Retrieve all historical chart data from database to calculate metrics.
    
    Returns:
        dict: Dictionary of historical chart data keyed by track_id (title|artist)
    """
    historical_data = {}
    
    try:
        print("\nRetrieving historical chart data from database...")
        conn = sqlite3.connect("spotify_charts.db")
        c = conn.cursor()
        
        # Check if the table exists
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='chart_entries'")
        if not c.fetchone():
            print("No historical data found (table doesn't exist)")
            conn.close()
            return historical_data
        
        # Get all chart entries from the database
        c.execute("SELECT chart_date, chart_type, position, title, artist FROM chart_entries")
        rows = c.fetchall()
        
        for row in rows:
            chart_date, chart_type, position, title, artist = row
            
            # Create a unique identifier for the track
            track_id = f"{title}|{artist}"
            
            # Create entry info
            entry_info = {
                'chart_date': chart_date,
                'chart_type': chart_type,
                'position': position
            }
            
            # Add to historical data dictionary
            if track_id not in historical_data:
                historical_data[track_id] = []
            
            historical_data[track_id].append(entry_info)
        
        conn.close()
        print(f"Retrieved historical data for {len(historical_data)} tracks")
        
    except Exception as e:
        print(f"❌ Error retrieving historical data: {e}")
        logging.error(f"Error retrieving historical data: {e}", exc_info=True)
    
    return historical_data

def generate_date_range(days=30):
    """Generate a list of dates going back a specified number of days."""
    today = datetime.datetime.now()
    date_list = ['latest']  # Start with 'latest' for today's chart
    
    # Then add the past days with actual dates
    for i in range(1, days):  # Start from 1 to skip today
        date = today - datetime.timedelta(days=i)
        date_list.append(date.strftime('%Y-%m-%d'))
    
    return date_list

def get_chart_types():
    """Return predefined chart types to scrape."""
    return ['global', 'usa']  # Default to both chart types

def main():
    try:
        print("\n" + "="*50)
        print("SPOTIFY CHARTS SCRAPER - IMPROVED VERSION".center(50))
        print("="*50)
        
        # Use both chart types by default
        chart_types = get_chart_types()
        print(f"Will scrape charts for: {', '.join(t.upper() for t in chart_types)}")
        
        # Only scrape the latest chart and 7 days of historical data for testing
        date_range = generate_date_range(8)  # Latest + 7 days
        print(f"Will scrape charts for: Latest + 7 days")
        
        driver = setup_driver()
        try:
            print("\n" + "-"*50)
            print("AUTHENTICATION".center(50))
            print("-"*50)
            login_to_spotify(driver, EMAIL, PASSWORD)
            
            print("\n" + "-"*50)
            print("DATA COLLECTION".center(50))
            print("-"*50)
            
            all_chart_data = []
            
            # First, get historical data from database
            historical_data = get_historical_data_from_db()
            
            for chart_type in chart_types:
                print(f"\n{'='*20} {chart_type.upper()} CHARTS {'='*20}")
                
                # Build the base URL based on chart type
                if chart_type == 'global':
                    base_url = "https://charts.spotify.com/charts/view/regional-global-daily"
                else:  # usa
                    base_url = "https://charts.spotify.com/charts/view/regional-us-daily"
                
                # Process each date in our range
                for chart_date in date_range:
                    date_display = chart_date if chart_date != 'latest' else 'latest (today)'
                    print(f"\n{'-'*20} Date: {date_display} {'-'*20}")
                    
                    url = f"{base_url}/{chart_date}"
                    print(f"URL: {url}")
                    
                    try:
                        # Scrape the chart data
                        data = scrape_spotify_charts(driver, url, chart_date, chart_type)
                        
                        if data:
                            all_chart_data.extend(data)
                            print(f"✓ Successfully scraped {len(data)} entries")
                        else:
                            print("No data scraped for this date")
                    except Exception as e:
                        print(f"❌ Failed to scrape {chart_type} chart for {chart_date}: {e}")
                        logging.error(f"Failed to scrape {chart_type} chart for {chart_date}: {e}")
                        continue
            
            # Process the chart data with historical information
            if all_chart_data:
                print("\n" + "-"*50)
                print("PROCESSING HISTORICAL DATA".center(50))
                print("-"*50)
                processed_data = process_chart_history(all_chart_data, historical_data)
                
                # Save to database
                save_to_database(processed_data)
                
                print("\n" + "-"*50)
                print("SUMMARY".center(50))
                print("-"*50)
                print(f"Total entries collected: {len(processed_data)}")
                print(f"Chart types processed: {', '.join(chart_types)}")
                print(f"Data has been saved to spotify_charts.db")
            else:
                print("\n❌ No chart data was collected.")
            
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

if __name__ == "__main__":
    main()