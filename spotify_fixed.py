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
                except Exception:
                    # If parsing fails, use current date
                    display_date = datetime.datetime.now().strftime('%Y-%m-%d')
                    print(f"Failed to parse date, using current date: {display_date}")
            except Exception:
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
                    # If position contains an arrow, extract the number before the arrow
                    if '→' in position_text:
                        position = int(position_text.split('→')[0].strip())
                    else:
                        # Try to extract just the number part
                        position = int(re.search(r'(\d+)', position_text).group(1))
                except Exception:
                    # Fall back to the entire text
                    try:
                        position = int(position_text)
                    except Exception:
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
                            except Exception:
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
                            except Exception:
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
                            artist = ""  # Make sure artist is empty string
                
                # Another approach: Look for links which are often artist names
                if not artist:
                    try:
                        artist_links = title_artist_cell.find_elements(By.TAG_NAME, "a")
                        if artist_links:
                            artist = ", ".join([a.text.strip() for a in artist_links if a.text.strip()])
                    except Exception:
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
                    except Exception:
                        pass
                
                if len(cells) >= 5:
                    try:
                        prev = cells[4].text.strip()
                    except Exception:
                        pass
                
                if len(cells) >= 6:
                    try:
                        streak = cells[5].text.strip()
                    except Exception:
                        pass
                
                if len(cells) >= 7:
                    try:
                        streams_text = cells[6].text.strip()
                        # Remove commas for numeric storage
                        streams = streams_text.replace(",", "")
                    except Exception:
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