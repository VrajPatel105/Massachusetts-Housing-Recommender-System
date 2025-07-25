import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
from datetime import datetime
import random
import re
import os
import undetected_chromedriver as uc
from webdriver_manager.chrome import ChromeDriverManager
import glob

# Using multiple user agents on a randomized way
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',
]

# Main class for Scraper :)   ~Vraj

class MultiPropertyZillowScraper:
    def __init__(self, headless=False):
        self.all_properties_data = []
        self.scraped_urls = set() # just to keep a track of scraped urls in a set to avoid duplicates
        self.last_scraped_url = None  # Track last scraped URL to avoid duplicates
        self.archived_data = []
        self.setup_driver(headless)        
           
    def setup_driver(self, headless):
        try:
            options = uc.ChromeOptions()
            if headless:
                options.add_argument("--headless=new")
            
            # Disable all the chrome optimization functionalities. 
            """
            I did this because on reaching pages after 5, if the chrome instance was not on the computer's screen, the chrome assumes that we are not looking
            and tries to minimize all the tasks, therefore it makes the content load even more slower. That's why i disabled all of those funcionalities.
            """
            options.add_argument("--disable-background-timer-throttling")
            options.add_argument("--disable-renderer-backgrounding") 
            options.add_argument("--disable-backgrounding-occluded-windows")
            options.add_argument("--disable-ipc-flooding-protection")
            
            # Force visibility
            options.add_argument("--disable-features=TranslateUI")
            options.add_argument("--disable-features=VizDisplayCompositor")
        
            options.add_argument(f'--user-agent={random.choice(USER_AGENTS)}')
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")

            self.driver = uc.Chrome(options=options, version_main=None)
            
        except Exception as e:
            options = Options()
            if headless:
                options.add_argument("--headless")
            
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)

    def check_driver_health(self):
        """A simple check to see if the driver is still responsive."""
        try:
            # Accessing a property like current_url is a lightweight way
            # to see if the connection to the browser is still alive.
            self.driver.current_url
            return True
        except Exception:
            # This will catch errors if the browser has crashed or closed.
            return False
        
    def scrape_multiple_properties(self, search_url, max_properties=50):
        """Switched to a tab-based model for faster, more stable scraping."""
        """Initially the method was to click on each element and scrape from that property. But the website is structured in a way that 
            if you click it and it fails once, the loaded content (properties on the main page) changes. So we have no way to track all of them.
            Therefore, I switched to a faster efficient way that instead of clicking redirects. 
            The main objective is to scroll the page and collect particular link in each element consisting of "/homedetails/".
            Once we have all the url in for the entire page, we go to each one of them on a new tab, scrape and come back to main tab, and repeat the process
            until all the url are scraped and then move on to next page. 
            One of the major reason to use the new tab technique was to not to disturb the website's main page loaded content.
        """
        print(f"Starting to scrape {max_properties} properties from search results...")
        
        self.driver.get(search_url)
        time.sleep(random.uniform(3.5, 5.5)) # trying to keep more time delay
        
        # few variables to track the progress
        properties_scraped = 0
        current_page = 1
        consecutive_failures = 0
        
        # the mian while loop that will run for each page.
        while properties_scraped < max_properties:
            print(f"\n=== PROCESSING PAGE {current_page} ===")
            
            try:
                # Wait for the main property list to be ready
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[1]/ul'))
                )
                print("--------------------Search results loaded----------------------")
            except:
                print("Likely Bot Detection. Search results failed to load. Stopping.")
                break
            
            # Scroll to ensure all list items are in the DOM
            print("Loading all properties on page...")
            self.scroll_to_load_all_properties()
            time.sleep(random.uniform(2,4))

            # Get the count and collect all property URLs from the page first
            property_count = self.get_property_count()
            all_links_on_page = self.get_all_links(property_count)
            print(f"Found {property_count} list items. Collected {len(all_links_on_page)} unique property links to process.")

            # just to test all the links
            # for i in all_links_on_page:
            #     print(i)

            #Get the handle of our main "home base" tab
            original_window = self.driver.current_window_handle

            #  Loop through the collected links
            for i, property_url in enumerate(all_links_on_page):
                
                # Stop if we've reached our target
                if properties_scraped >= max_properties:
                    print(f"Reached target of {max_properties} properties.")
                    break
                
                print(f"\n--> Processing link {i + 1} / {len(all_links_on_page)} (Total Scraped So Far: {properties_scraped})")
                
                #Efficiency check: skip if we have already scraped this URL from a previous page
                if property_url in self.scraped_urls:
                    print(f"  - Skipping duplicate URL found on a previous page: {property_url}")
                    continue

                try:
                    # Open a new tab
                    time.sleep(random.uniform(3,6)) # Wait for the new page to load
                    self.driver.switch_to.new_window('tab')
                    
                    # Navigate to the property URL in the new tab
                    self.driver.get(property_url)
                    time.sleep(1)

                    # Scrape all the data from the new tab -> Main function that scrapes data
                    property_data = self.extract_complete_property_data()

                    if property_data:
                        self.all_properties_data.append(property_data)
                        self.scraped_urls.add(property_url) # Add to our set of scraped URLs
                        properties_scraped += 1
                        consecutive_failures = 0
                        print(f"  ✅ Successfully scraped property {properties_scraped}")
                        
                        # Save a checkpoint every 5 properties
                        if properties_scraped % 10 == 0:
                            self.save_progress_checkpoint("current_scrape", properties_scraped)
                
                except Exception as e:
                    print(f"  ❌ An error occurred while scraping {property_url}: {e}")
                    consecutive_failures += 1
                    if consecutive_failures >= 5:
                        print(" Too many consecutive failures. Stopping scrape.")
                        # This break will exit the for loop
                        break 
                
                finally:
                    # It ensures we always clean up our tabs.
                    # Close the current (property) tab
                    self.driver.close()
                    
                    # Switch focus back to the original "home base" tab
                    self.driver.switch_to.window(original_window)
                    
                    # A brief pause to ensure stability
                    time.sleep(random.uniform(0.5, 1.5))
            
            # Check if we need to stop due to reaching the max properties or too many failures
            if properties_scraped >= max_properties or consecutive_failures >= 5:
                break
            
            # After processing all links on this page, go to the next page
            print("\nFinished all links on this page. Attempting to navigate to the next page...")
            time.sleep(5)
            try:
                if current_page >= 20:
                    print(f"⚠️ Reached Zillow's maximum page limit (20). Stopping pagination.")
                    break
                elif self.go_to_next_page():
                    current_page += 1
                    consecutive_failures = 0
                    time.sleep(random.uniform(2.5, 3.5))
                else:
                    print("❌ No more pages available. End of results.")
                    break
            except Exception as e:
                print(f"❌ Page navigation failed: {e}")
                break
        
        print(f"\n Scraping completed! Total properties successfully scraped: {properties_scraped}")
        return self.all_properties_data

    def get_all_links(self, property_count):
        print("We are now inside the get_all_links function.")
        all_property_links = []

        # Use a for loop to iterate from 1 to property_count
        for idx in range(1, property_count + 1):
            try:
                # Construct the XPath for the current property container
                property_xpath = f'/html/body/div[1]/div/div[2]/div/div/div[1]/div[1]/ul/li[{idx}]'
                property_element = self.driver.find_element(By.XPATH, property_xpath)
                
                # Scroll the specific property element into the middle of the view.
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", property_element)
                
                #  Wait for the link *inside* the element to become present.
                # This is much more reliable than a fixed sleep. We'll wait up to 5 seconds.
                wait = WebDriverWait(property_element, 5) # Wait is scoped to the specific element
                link_element = wait.until(
                    EC.presence_of_element_located((By.XPATH, ".//a[contains(@href, '/homedetails/')]")) # we are specifically looking for links containing /homedetails/ in the string
                )
                
                # Now that we know the link exists, get the URL.
                property_url = link_element.get_attribute('href')
                
                if property_url:
                    all_property_links.append(property_url)
                
            except Exception as e:
                # This is expected for ads or other non-standard items.
                print(f"  - Skipping index {idx}.")
                continue # Move on to the next one.

        return all_property_links

    def scroll_to_load_all_properties(self):
        """Scroll down to load all properties via lazy loading"""
        try:
            print("  Scrolling to load all properties...")
            
            # Get initial count
            initial_count = len(self.driver.find_elements(By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[1]/ul/li'))
            print(f"  Initial properties loaded: {initial_count}")
            
            # Scroll down gradually to trigger lazy loading
            for i in range(5):  # 5 scroll steps
                scroll_position = (i + 1) * 800  # Scroll 800px each time
                self.driver.execute_script(f"window.scrollTo(0, {scroll_position});")
                time.sleep(random.uniform(5, 7))
                
                # Check if more properties loaded
                current_count = len(self.driver.find_elements(By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[1]/ul/li'))
                if current_count > initial_count:
                    print(f"  Loaded {current_count - initial_count} more properties")
                    initial_count = current_count
            
            # Scroll back to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2) 
            
            final_count = len(self.driver.find_elements(By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[1]/ul/li'))
            print(f"  Total properties loaded: {final_count}")
            
            # more time delay because zillow uses lazy loading feature.
            time.sleep(7)
            
            return final_count
            
        except Exception as e:
            print(f" Error during scrolling: {e}")
            return 0

    def get_property_count(self):
        """Simple property count - just count li elements"""
        try:
            elements = self.driver.find_elements(By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[1]/ul/li')
            return len(elements)
        except Exception as e:
            print(f"Error counting properties: {e}")
            return 0
        
    def go_to_next_page(self):
        """
        Navigate to the next page using a selector that finds the button
        by its title, not its position.
        """
        try:
            print(" Looking for the 'Next page' button...")

            # This works regardless of the button's position on the page.
            next_button_xpath = "//a[@title='Next page']"

            # Use WebDriverWait to handle cases where the page is still loading.
            wait = WebDriverWait(self.driver, 5)
            next_button = wait.until(
                EC.presence_of_element_located((By.XPATH, next_button_xpath))
            )

            # On Zillow, the button still exists on the last page but is disabled.
            # We must check the 'aria-disabled' attribute to know when to stop.
            if next_button.get_attribute('aria-disabled') == 'true':
                print("  ✓ 'Next page' button is disabled. This is the last page of results.")
                return False

            # If we're here, the button exists and is enabled. Let's click it.
            print("  ✓ Found enabled 'Next page' button. Clicking to navigate...")

            # Scroll the button into view to ensure it's clickable.
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
            time.sleep(random.uniform(0.5,1))  # A brief pause after scrolling.

            # Use a JavaScript click, which is often more reliable than a standard .click().
            self.driver.execute_script("arguments[0].click();", next_button)

            # Wait for the next page to load. A good way to confirm this is to
            # wait for the main property list to be present again.
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[1]/ul'))
            )

            print("  ✅ mSuccessfully navigated to the next page.")
            return True

        except TimeoutException:
            # This will happen if the WebDriverWait times out after 5 seconds.
            # It means the "Next page" button was not found at all.
            print("  'Next page' button not found. Assuming end of results.")
            return False

        except Exception as e:
            # Catch any other unexpected errors.
            print(f" An unexpected error occurred in go_to_next_page: {e}")
            return False
             
    def save_progress_checkpoint(self, county_name, current_count):
        """Save progress with better file management"""
        if len(self.all_properties_data) % 50 == 0 and len(self.all_properties_data) > 0:  # Every 50 instead of 10
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save in current city directory instead of main folder
            checkpoint_filename = f"checkpoint_{county_name}_{current_count}props_{timestamp}.json"
            
            # Remove old checkpoints (keep only latest 2)

            old_checkpoints = glob.glob(f"checkpoint_{county_name}_*props_*.json")
            if len(old_checkpoints) > 2:
                old_checkpoints.sort()
                for old_file in old_checkpoints[:-2]:  # Keep only 2 newest
                    try:
                        os.remove(old_file)
                    except:
                        pass
            
            with open(checkpoint_filename, 'w') as f:
                json.dump(self.all_properties_data, f, indent=2)
            
            print(f"🔄 CHECKPOINT: {checkpoint_filename}")
    
    def extract_complete_property_data(self):
        """Extract all property data from current property page - optimized version"""
        try:
            print("Starting property data extraction...")
            
            property_data = {
                'url': self.driver.current_url,
                'image_url': 'N/A',
                'scraped_at': datetime.now().isoformat(),
                
                'price': 'N/A',
                'beds': 'N/A', 
                'baths': 'N/A',
                'sqft': 'N/A',
                'sqft_lot': 'N/A',
                'address': 'N/A',
                'estimated_monthly_payment': 'N/A',
                'property_type': 'N/A',
                'price_per_sqft': 'N/A',
                'year_built': 'N/A',
                'region':'N/A',
                
                'interior_features': [],
                'other_rooms': [],
                'appliances': [],
                'utilities': 'N/A',
                'parking': 'N/A',
                
                'walk_score':'N/A',
                'bike_score':'N/A',
                'transit_score':'N/A',
                
                'elementary_school': {'name': 'N/A', 'distance': 'N/A'},
                'middle_school': {'name': 'N/A', 'distance': 'N/A'},
                'high_school': {'name': 'N/A','distance': 'N/A'},
    
                'flood_risk': 'N/A',
                'fire_risk': 'N/A',
                'wind_risk':'N/A',
                'air_risk':'N/A',
                'heat_risk': 'N/A',

                'nearby_cities': [],
                'property_history': 'N/A'
            }

            # calling all the functions for data scraping
            try:
                self.extract_property_image_url(property_data)
                print('- Property Image URL Scraping done')
            except Exception as e:
                print(f"  - Error in image URL: {e}")
            try:
                self.extract_price_and_basic_info(property_data)
                print('- Basic Information Scraping Done')
            except Exception as e:
                print(f"  - Error in basic info: {e}")
            try:
                self.extract_property_features_detailed(property_data)
                print('- Property Features Scraping done')
            except Exception as e:
                print(f"  - Error in features: {e}")
            try:
                self.extract_neighborhood_scores_detailed(property_data)
                print('- Neighbourhood Features Scraping done')
            except Exception as e:
                print(f"  - Error in features: {e}")
            try:
                self.extract_schools_detailed(property_data)
                print('- School Features Scraping done')
            except Exception as e:
                print(f"  - Error in features: {e}")
            try:
                self.extract_environmental_risks(property_data)
                print('- Environmental Features Scraping done')
            except Exception as e:
                print(f"  - Error in features: {e}")
            try:
                self.extract_market_data_detailed(property_data)
                print('- Market Features Scraping done')
            except Exception as e:
                print(f"  - Error in features: {e}")
            try:
                self.extract_nearby_cities(property_data)
                print('- Nearby Cities Features Scraping done')
            except Exception as e:
                print(f"  - Error in features: {e}")
            
            print("Property data extraction completed!")
            return property_data
            
        except Exception as e:
            print(f"Error in extraction: {e}")
            return None
    
    def extract_property_image_url(self, property_data):
        """Extract first property image URL - SAFE approach"""
        try:
            print("  - Extracting property image URL...")
            
            property_data['image_url'] = 'N/A'
            
            # Look for image in common locations (most reliable)
            image_selectors = [
                # Primary image selectors (most common)
                'img[data-testid*="property-image"]',
                'img[alt*="property"]',
                'img[src*="photos.zillowstatic.com"]',
                
                # Fallback selectors
                '.media-stream img:first-child',
                '.photo-carousel img:first-child',
                'picture img',
                
                # Generic selectors (last resort)
                'section img:first-child',
                'main img:first-child'
            ]
            
            for selector in image_selectors:
                try:
                    image_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    image_url = image_element.get_attribute('src')
                    
                    # Validate URL
                    if image_url and self.is_valid_zillow_image_url(image_url):
                        property_data['image_url'] = image_url
                        print(f"  Found image URL: {image_url[:50]}...")
                        return
                        
                except Exception:
                    continue
            
            #  Search page source for image URLs (backup)
            try:
                page_source = self.driver.page_source
                
                # Look for Zillow image URL patterns in page source
                image_patterns = [
                    r'https://photos\.zillowstatic\.com/[^"\'>\s]+',
                    r'https://[^"\'>\s]*zillow[^"\'>\s]*\.jpg',
                    r'https://[^"\'>\s]*zillow[^"\'>\s]*\.webp'
                ]
                
                for pattern in image_patterns:
                    matches = re.findall(pattern, page_source, re.I)
                    if matches:
                        # Take first valid image URL
                        for url in matches:
                            if self.is_valid_zillow_image_url(url):
                                property_data['image_url'] = url
                                print(f" Found image URL via page source: {url[:50]}...")
                                return
                                
            except Exception as e:
                print(f" Page source image search failed: {e}")
            
            print(" No property image URL found")
            
        except Exception as e:
            print(f" Error extracting image URL: {e}")
            property_data['image_url'] = 'N/A'

    def is_valid_zillow_image_url(self, url):
        """Validate if URL is a proper Zillow image URL"""
        if not url or len(url) < 10:
            return False
            
        # Check for Zillow image patterns
        valid_patterns = [
            'photos.zillowstatic.com',
            'zillow.com',
            '.jpg',
            '.jpeg',
            '.webp',
            '.png'
        ]
        
        # Must contain Zillow domain and image extension
        has_zillow = any(pattern in url.lower() for pattern in ['zillow', 'zillowstatic'])
        has_image_ext = any(url.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.webp', '.png'])
        
        # Exclude obviously bad URLs
        bad_patterns = ['icon', 'logo', 'avatar', 'blank', 'placeholder']
        has_bad_pattern = any(bad in url.lower() for bad in bad_patterns)
        
        return has_zillow and has_image_ext and not has_bad_pattern and len(url) > 30

    def extract_price_and_basic_info(self, property_data):
        self.extract_price_advanced(property_data)
        self.extract_basic_info_advanced(property_data)
    
    def extract_price_advanced(self, property_data):
        price_strategies = [
            ('CSS', 'span[data-testid="price"]'),
            ('CSS', '.notranslate'),
            ('CSS', 'h3 span'),
            ('CSS', 'span.Text-c11n-8-100-1__sc-aiai24-0'),
            ('XPATH', "//span[contains(@class, 'Text') and contains(text(), '$')]"),
            ('XPATH', "//h3//span[contains(text(), '$')]"),
        ]
        
        for strategy_type, selector in price_strategies:
            try:
                if strategy_type == 'CSS':
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                else:
                    elements = self.driver.find_elements(By.XPATH, selector)
                
                for element in elements:
                    text = element.text.strip()
                    price_match = re.match(r'^\$[\d,]+(?:\.\d{2})?$', text)
                    if price_match:
                        property_data['price'] = text
                        return
            except:
                continue
    
    def extract_basic_info_advanced(self, property_data):
        # Reset values
        property_data['beds'] = 'N/A'
        property_data['baths'] = 'N/A'
        property_data['sqft'] = 'N/A'
        
        try:
            # Strategy 1: Use the data-testid we discovered (most reliable)
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, '[data-testid="bed-bath-sqft-facts"]')
                text = element.text.strip()
                
                # Extract beds, baths, sqft from the structured text
                bed_match = re.search(r'(\d+)\s*beds?', text, re.I)
                bath_match = re.search(r'(\d+(?:\.\d+)?)\s*baths?', text, re.I)
                sqft_match = re.search(r'([\d,]+)\s*sqft', text, re.I)
                
                if bed_match:
                    property_data['beds'] = bed_match.group(1)
                if bath_match:
                    property_data['baths'] = bath_match.group(1)
                if sqft_match:
                    property_data['sqft'] = sqft_match.group(1)
                    
            except Exception as e:
                print(f"Primary strategy failed: {e}")
            
            # Strategy 2: Fallback to other elements if primary failed
            if (property_data['beds'] == 'N/A' or 
                property_data['baths'] == 'N/A' or 
                property_data['sqft'] == 'N/A'):
                
                print("Primary strategy incomplete, trying fallback...")
                
                # Try other fact containers
                fallback_selectors = [
                    '[data-testid="property-facts"]',
                    '[data-testid="facts-container"]',
                    '.summary-container',
                    'section[aria-label*="facts"]'
                ]
                
                for selector in fallback_selectors:
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for element in elements:
                            text = element.text.strip()
                            
                            if property_data['beds'] == 'N/A':
                                bed_match = re.search(r'(\d+)\s*bed', text, re.I)
                                if bed_match and 1 <= int(bed_match.group(1)) <= 10:
                                    property_data['beds'] = bed_match.group(1)
                            
                            if property_data['baths'] == 'N/A':
                                bath_match = re.search(r'(\d+(?:\.\d+)?)\s*bath', text, re.I)
                                if bath_match and 0.5 <= float(bath_match.group(1)) <= 10:
                                    property_data['baths'] = bath_match.group(1)
                            
                            if property_data['sqft'] == 'N/A':
                                sqft_match = re.search(r'([\d,]+)\s*sqft', text, re.I)
                                if sqft_match:
                                    sqft_value = int(sqft_match.group(1).replace(',', ''))
                                    if 300 <= sqft_value <= 20000:
                                        property_data['sqft'] = sqft_match.group(1)
                            
                            # Break if we found everything
                            if (property_data['beds'] != 'N/A' and 
                                property_data['baths'] != 'N/A' and 
                                property_data['sqft'] != 'N/A'):
                                break
                        
                        if (property_data['beds'] != 'N/A' and 
                            property_data['baths'] != 'N/A' and 
                            property_data['sqft'] != 'N/A'):
                            break
                            
                    except Exception as e:
                        continue
            
            # Strategy 3: Page source JSON as last resort
            if (property_data['beds'] == 'N/A' or 
                property_data['baths'] == 'N/A' or 
                property_data['sqft'] == 'N/A'):
                
                print("Trying JSON extraction from page source...")
                try:
                    page_source = self.driver.page_source
                    
                    if property_data['beds'] == 'N/A':
                        for pattern in [r'"bedrooms"[:\s]*(\d+)', r'"beds"[:\s]*(\d+)']:
                            match = re.search(pattern, page_source, re.I)
                            if match:
                                bed_value = int(match.group(1))
                                if 1 <= bed_value <= 10:
                                    property_data['beds'] = str(bed_value)
                                    break
                    
                    if property_data['baths'] == 'N/A':
                        for pattern in [r'"bathrooms"[:\s]*(\d+(?:\.\d+)?)', r'"baths"[:\s]*(\d+(?:\.\d+)?)']:
                            match = re.search(pattern, page_source, re.I)
                            if match:
                                bath_value = float(match.group(1))
                                if 0.5 <= bath_value <= 10:
                                    property_data['baths'] = str(bath_value)
                                    break
                    
                    if property_data['sqft'] == 'N/A':
                        for pattern in [r'"livingArea"[:\s]*(\d+)', r'"floorSize"[:\s]*(\d+)']:
                            match = re.search(pattern, page_source, re.I)
                            if match:
                                sqft_value = int(match.group(1))
                                if 300 <= sqft_value <= 20000:
                                    property_data['sqft'] = f"{sqft_value:,}"
                                    break
                                    
                except Exception as e:
                    print(f"JSON extraction failed: {e}")
            
            # Address extraction
            address_strategies = [
                ('CSS', 'h1[data-testid="street-address"]'),
                ('CSS', 'h1'),
            ]
            
            for strategy_type, selector in address_strategies:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    text = element.text.strip()
                    if any(indicator in text.lower() for indicator in ['st', 'ave', 'rd', 'dr', 'ma']):
                        property_data['address'] = text
                        break
                except:
                    continue
            
            # Page source extraction for other data
            page_text = self.driver.page_source
            
            type_match = re.search(r'(single.family|condo|townhouse|multi.family)', page_text, re.I)
            if type_match:
                property_data['property_type'] = type_match.group(1)
            
            year_patterns = [
                r'Built in (\d{4})',
                r'built[:\s]+(\d{4})',
                r'year[:\s]+(\d{4})'
            ]
            for pattern in year_patterns:
                year_match = re.search(pattern, page_text, re.I)
                if year_match:
                    property_data['year_built'] = year_match.group(1)
                    break
            
            price_sqft_patterns = [
                r'\$([\d,]+)/sqft',
                r'\$([\d,]+)\s*price/sqft',
                r'price/sqft[:\s]+\$([\d,]+)',
                r'\$([\d,]+)\s*/\s*sqft'
            ]
            for pattern in price_sqft_patterns:
                price_sqft_match = re.search(pattern, page_text, re.I)
                if price_sqft_match:
                    price_value = price_sqft_match.group(1).replace(',', '')
                    property_data['price_per_sqft'] = f"${price_value}/sqft"
                    break
            
            lot_patterns = [
                r'([\d,]+)\s*Square\s*Feet\s*Lot',  # "4,373 Square Feet Lot"
                r'(\d+\.?\d*)\s*Acres\s*Lot',       # "0.31 Acres Lot"
                r'(\d+\.?\d*)\s*Acres',
                r'(\d+\.?\d*)\s*acres',
                r'lot[:\s]*([\d,.]+)\s*(sq\s*ft|sqft|square\s*feet|acres)',
                r'([\d,.]+)\s*(acres|sq\s*ft|sqft|square\s*feet)\s*lot',
                r'lot\s*size[:\s]*([\d,.]+)\s*(sq\s*ft|sqft|square\s*feet|acres)',
                r'([\d,.]+)\s*square\s*feet\s*lot',
                r'([\d,.]+)\s*sq\s*ft\s*lot',
                r'([\d,.]+)\s*sqft\s*lot',
                r'lot[:\s]*([\d,.]+)',
                r'Lot\s*:\s*([\d,.]+)',
                r'Property\s*size[:\s]*([\d,.]+)\s*(sq\s*ft|sqft|square\s*feet|acres)'
            ]
            
            for pattern in lot_patterns:
                lot_match = re.search(pattern, page_text, re.I)
                if lot_match:
                    if len(lot_match.groups()) == 2:
                        size = lot_match.group(1)
                        unit = lot_match.group(2)
                        if 'acres' in unit.lower():
                            property_data['sqft_lot'] = f"{size} Acres"
                        else:
                            property_data['sqft_lot'] = f"{size} sqft"
                    else:
                        size = lot_match.group(1)
                        # Check if it's "Square Feet Lot" or "Acres Lot" pattern
                        if 'square feet lot' in lot_match.group(0).lower():
                            property_data['sqft_lot'] = f"{size} sqft"
                        elif 'acres lot' in lot_match.group(0).lower():
                            property_data['sqft_lot'] = f"{size} Acres"
                        else:
                            # Default to sqft if no unit specified
                            property_data['sqft_lot'] = f"{size} sqft"
                    break
                            
        except Exception as e:
            print(f"❌ Basic info extraction failed: {e}")

    def extract_property_features_detailed(self, property_data):
        try:
            print("  - Scrolling to middle of page...")
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(random.uniform(1, 2))
            
            print("  - Looking for expandable buttons...")
            expandable_buttons = self.driver.find_elements(By.XPATH, "//button[contains(text(), 'Show more')]")
            for button in expandable_buttons:
                try:
                    self.driver.execute_script("arguments[0].click();", button)
                    time.sleep(0.5)
                except:
                    pass

            print("  - Extracting features from page source...")
            page_text = self.driver.page_source.lower()  # Convert to lowercase once
            
            # Compile all regex patterns once for better performance
            compiled_patterns = {
                'interior_features': [
                    re.compile(pattern, re.I) for pattern in [
                        r'hardwood\s+floors?', r'granite\s+countertops?', r'stainless\s+steel', 
                        r'tile\s+floors?', r'carpet', r'laminate', r'marble', r'walk-in\s+closet', 
                        r'bay\s+window', r'skylight', r'fireplace', r'built-in\s+shelves?', 
                        r'crown\s+molding', r'vaulted\s+ceiling'
                    ]
                ],
                'other_rooms': [
                    re.compile(pattern, re.I) for pattern in [
                        r'dining\s+room', r'family\s+room', r'living\s+room', r'bonus\s+room', 
                        r'office', r'den', r'study', r'library', r'sunroom', r'basement', 
                        r'attic', r'laundry\s+room', r'mud\s+room', r'pantry', r'walk-in\s+pantry'
                    ]
                ],
                'appliances': [
                    re.compile(pattern, re.I) for pattern in [
                        r'dishwasher', r'refrigerator', r'microwave', r'oven', r'range', 
                        r'cooktop', r'disposal', r'washer', r'dryer', r'freezer', 
                        r'wine\s+cooler', r'ice\s+maker'
                    ]
                ]
            }
            
            # Single pass extraction using sets for O(1) lookups
            interior_features = set()
            other_rooms = set()
            appliances = set()
            
            # Process each category in one pass
            for category, patterns in compiled_patterns.items():
                target_set = locals()[category]  # Get the corresponding set
                max_items = 5 if category == 'interior_features' else 3
                
                for pattern in patterns:
                    if len(target_set) >= max_items:
                        break
                    matches = pattern.findall(page_text)
                    for match in matches:
                        if len(target_set) >= max_items:
                            break
                        target_set.add(match.lower())
            
            # Convert sets back to lists
            property_data['interior_features'] = list(interior_features)
            property_data['other_rooms'] = list(other_rooms)
            property_data['appliances'] = list(appliances)
            
            # Utilities extraction - compile patterns once and search once
            utilities = {}
            utility_compiled_patterns = {
                'Electric': re.compile(r'Electric:\s*([^<\n]+)', re.I),
                'Sewer': re.compile(r'Sewer:\s*([^<\n]+)', re.I),
                'Water': re.compile(r'Water:\s*([^<\n]+)', re.I),
                'Utilities': re.compile(r'Utilities for property:\s*([^<\n]+)', re.I)
            }
            
            for utility_type, pattern in utility_compiled_patterns.items():
                match = pattern.search(page_text)
                if match:
                    utilities[utility_type] = match.group(1).strip()
            
            property_data['utilities'] = utilities if utilities else 'N/A'
            
            # Parking extraction - compile patterns once
            parking = {}
            parking_compiled_patterns = {
                'total_spaces': re.compile(r'Total spaces:\s*(\d+)', re.I),
                'garage_spaces': re.compile(r'Garage spaces:\s*(\d+)', re.I),
                'parking_features': re.compile(r'Parking features:\s*([^<\n]+)', re.I),
                'uncovered_spaces': re.compile(r'Has uncovered spaces:\s*([^<\n]+)', re.I)
            }
            
            for parking_type, pattern in parking_compiled_patterns.items():
                match = pattern.search(page_text)
                if match:
                    parking[parking_type] = match.group(1).strip()
            
            property_data['parking'] = parking if parking else 'N/A'
            print("  - Features extraction completed")
            
        except Exception as e:
            print(f"  - Error in features extraction: {e}")
            property_data['interior_features'] = []
            property_data['other_rooms'] = []
            property_data['appliances'] = []
            property_data['utilities'] = 'N/A'
            property_data['parking'] = 'N/A'

        # monthly payment section
        try:
            payment_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'Monthly') or contains(text(), 'monthly') or contains(text(), 'Payment')]")
            
            for element in payment_elements:
                try:
                    container = element.find_element(By.XPATH, "./..")
                    container_text = container.text
                    
                    payment_match = re.search(r'\$[\d,]+(?:/mo|/month|\s+monthly)', container_text, re.I)
                    if payment_match:
                        property_data['estimated_monthly_payment'] = payment_match.group(0)
                        break
                        
                except:
                    continue
                
        except Exception as e:
            pass
    
    def extract_neighborhood_scores_detailed(self, property_data):
        """OPTIMIZED: Fast extraction using specific selectors"""
        try:

            if not self.check_driver_health():
                print("  - Driver unhealthy, skipping schools extraction")
                return
            
            print("  - Looking for neighborhood scores...")
            
            # Initialize scores
            property_data['walk_score'] = 'N/A'
            property_data['bike_score'] = 'N/A'
            property_data['transit_score'] = 'N/A'
            
            # Quick scroll to scores section (around 60-70% down the page)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.65);")
            time.sleep(random.uniform(1.5, 2))  # Wait for content to load
            
            # Strategy 1: Use the specific container you found
            try:
                # Your specific container selector
                container_selectors = [
                    '#wrapper > div:nth-child(2) > div.styles__StyledContentWrapper-fshdp-8-111-1__sc-1syvsv7-0.cuZQjs.layout-wrapper > section > div > div.layout-content-container > div.layout-static-column-container > div.Flex-c11n-8-111-1__sc-n94bjd-0.ilxGya > div > div:nth-child(24) > div > div > div.styles__StyledScoresContainer-fshdp-8-111-1__sc-1kythi0-1.hQqCYo > div',
                    
                    # More generic fallback selectors
                    '[class*="StyledScoresContainer"] > div',
                    '[class*="ScoresContainer"]',
                    'div[class*="hQqCYo"]'
                ]
                
                scores_container = None
                for selector in container_selectors:
                    try:
                        scores_container = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if scores_container:
                            break
                    except:
                        continue
                
                if scores_container:
                    container_text = scores_container.text
                    
                    # Extract all scores from the container text at once
                    score_patterns = {
                        'walk_score': [
                            r'Walk Score[®]?\s*(\d+)',
                            r'(\d+)\s*/?\s*100\s*Walk',
                            r'Walk\s*Score\s*(\d+)',
                            r'(\d+)\s*Walk'
                        ],
                        'bike_score': [
                            r'Bike Score[®]?\s*(\d+)',
                            r'(\d+)\s*/?\s*100\s*Bike',
                            r'Bike\s*Score\s*(\d+)',
                            r'(\d+)\s*Bike'
                        ],
                        'transit_score': [
                            r'Transit Score[®]?\s*(\d+)',
                            r'(\d+)\s*/?\s*100\s*Transit',
                            r'Transit\s*Score\s*(\d+)',
                            r'(\d+)\s*Transit'
                        ]
                    }
                    
                    # Extract scores efficiently
                    for score_type, patterns in score_patterns.items():
                        for pattern in patterns:
                            try:
                                match = re.search(pattern, container_text, re.I)
                                if match:
                                    score = int(match.group(1))
                                    if 0 <= score <= 100:
                                        property_data[score_type] = f"{score}/100"
                                        break
                            except (ValueError, AttributeError):
                                continue
                        
                        # Stop looking for this score if we found it
                        if property_data[score_type] != 'N/A':
                            continue
                
            except Exception as e:
                print(f"    Container approach failed: {e}")
            
            # Strategy 2: Quick direct search if container approach failed
            if (property_data['walk_score'] == 'N/A' or 
                property_data['bike_score'] == 'N/A' or 
                property_data['transit_score'] == 'N/A'):
                
                try:
                    # Get page source once for fast regex search
                    page_source = self.driver.page_source
                    
                    # Quick regex patterns for remaining scores
                    remaining_patterns = {
                        'walk_score': r'(?:Walk\s*Score|Walkability)[:\s]*(\d+)',
                        'bike_score': r'(?:Bike\s*Score|Bikeability)[:\s]*(\d+)',
                        'transit_score': r'(?:Transit\s*Score|Transit)[:\s]*(\d+)'
                    }
                    
                    for score_type, pattern in remaining_patterns.items():
                        if property_data[score_type] == 'N/A':
                            try:
                                match = re.search(pattern, page_source, re.I)
                                if match:
                                    score = int(match.group(1))
                                    if 0 <= score <= 100:
                                        property_data[score_type] = f"{score}/100"
                            except (ValueError, AttributeError):
                                continue
                                
                except Exception as e:
                    print(f"    Page source search failed: {e}")
            
            # Strategy 3: Ultra-quick element search for any remaining missing scores
            missing_scores = [score for score in ['walk_score', 'bike_score', 'transit_score'] 
                            if property_data[score] == 'N/A']
            
            if missing_scores:
                try:
                    # Look for any elements containing score keywords
                    score_keywords = ['walk', 'bike', 'transit', 'score']
                    for keyword in score_keywords:
                        try:
                            elements = self.driver.find_elements(By.XPATH, f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{keyword}')]")
                            
                            for element in elements[:3]:  # Only check first 3 matches
                                try:
                                    element_text = element.text
                                    # Quick check for score patterns
                                    score_match = re.search(r'(\d+)(?:/100)?', element_text)
                                    if score_match:
                                        score = int(score_match.group(1))
                                        if 0 <= score <= 100:
                                            # Determine which score type based on context
                                            if 'walk' in element_text.lower() and property_data['walk_score'] == 'N/A':
                                                property_data['walk_score'] = f"{score}/100"
                                            elif 'bike' in element_text.lower() and property_data['bike_score'] == 'N/A':
                                                property_data['bike_score'] = f"{score}/100"
                                            elif 'transit' in element_text.lower() and property_data['transit_score'] == 'N/A':
                                                property_data['transit_score'] = f"{score}/100"
                                except:
                                    continue
                                
                        except:
                            continue
                            
                except Exception as e:
                    print(f"    Element search failed: {e}")
            
            # Report results
            scores_found = sum(1 for score in [property_data['walk_score'], property_data['bike_score'], property_data['transit_score']] 
                            if score != 'N/A')
            
            print("  - Neighborhood scores extraction completed")
            
        except Exception as e:
            print(f"  - Error in neighborhood scores extraction: {e}")
            property_data['walk_score'] = 'N/A'
            property_data['bike_score'] = 'N/A'
            property_data['transit_score'] = 'N/A'
    
    def extract_schools_detailed(self, property_data):
        """REVERTED: Simple school extraction with FIXED distance patterns"""
        try:

            if not self.check_driver_health():
                print("  - Driver unhealthy, skipping schools extraction")
                return
            
            print("  - Looking for school information...")
            
            # Quick scroll to schools area
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.6);")
            time.sleep(1)
            
            # Get page source once for faster processing
            page_source = self.driver.page_source
            
            # Simple text-based extraction for each school type
            school_types = ['elementary', 'middle', 'high']
            
            for school_type in school_types:
                # Look for school name patterns in the page source
                name_patterns = [
                    rf'([A-Z][a-zA-Z\s]+?)\s*{school_type.title()}',
                    rf'([A-Z][a-zA-Z\s]+?)\s*School.*?{school_type}',
                    rf'{school_type.title()}[:\s]*([A-Z][a-zA-Z\s]+)',
                ]
                
                school_name = 'N/A'
                for pattern in name_patterns:
                    name_match = re.search(pattern, page_source, re.I)
                    if name_match:
                        potential_name = name_match.group(1).strip()
                        # ADD THIS: Filter out common non-school text
                        bad_keywords = ['check with', 'contact', 'verify', 'call', 'please', 'applicable', 'district', 'information', 'the applicable']
                        if (len(potential_name) > 3 and len(potential_name) < 30 and 
                            not any(bad in potential_name.lower() for bad in bad_keywords)):
                            school_name = potential_name
                            break
            
                school_distance = 'N/A'
                
                # Try multiple distance patterns - FIXED for actual format
                distance_patterns = [
                    # Pattern 1: "Distance: X.X mi" format (from your image)
                    rf'{school_type}.*?Distance:\s*(\d+\.?\d*)\s*mi',
                    
                    # Pattern 2: School name followed by distance (if we found the name)
                    rf'{re.escape(school_name)}.*?Distance:\s*(\d+\.?\d*)\s*mi' if school_name != 'N/A' else None,
                    
                    # Pattern 3: Alternative formats
                    rf'{school_type}.*?(\d+\.?\d*)\s*mi',
                    
                    # Pattern 4: For middle school, also try "junior" or "k-8"
                    rf'(?:junior|k-8).*?Distance:\s*(\d+\.?\d*)\s*mi' if school_type == 'middle' else None,
                ]
                
                # Remove None patterns
                distance_patterns = [p for p in distance_patterns if p is not None]
                
                for pattern in distance_patterns:
                    try:
                        distance_matches = re.findall(pattern, page_source, re.I)
                        if distance_matches:
                            # Take the first reasonable distance
                            for distance in distance_matches:
                                try:
                                    distance_float = float(distance)
                                    if 0.1 <= distance_float <= 50:
                                        school_distance = f"{distance} mi"
                                        break
                                except ValueError:
                                    continue
                            
                            if school_distance != 'N/A':
                                break
                    except re.error:
                        continue
                
                # Save the school data
                property_data[f'{school_type}_school'] = {
                    'name': school_name,
                    'distance': school_distance
                }
            
            print("  - School extraction completed")
            
        except Exception as e:
            print(f"  - Error in school extraction: {e}")
    
    def extract_environmental_risks(self, property_data):
        try:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(2.5, 3.5))
            
            property_data['flood_risk'] = 'N/A'
            property_data['fire_risk'] = 'N/A'
            property_data['wind_risk'] = 'N/A'
            property_data['air_risk'] = 'N/A'
            property_data['heat_risk'] = 'N/A'
            
            try:
                climate_section = self.driver.find_element(By.XPATH, "//*[contains(text(), 'Climate risks')]")
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", climate_section)
                time.sleep(random.uniform(2, 3))
            except:
                pass
            
            risk_mappings = {
                'flood': 'flood_risk',
                'fire': 'fire_risk', 
                'wind': 'wind_risk',
                'air': 'air_risk',
                'heat': 'heat_risk'
            }
            
            for risk_type, risk_key in risk_mappings.items():
                try:
                    elements = self.driver.find_elements(By.XPATH, f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{risk_type} factor')]")
                    
                    for element in elements:
                        try:
                            container = element.find_element(By.XPATH, "./../../..")
                            container_text = container.text
                            
                            level_match = re.search(r'(Minimal|Minor|Moderate|Major|Severe)', container_text, re.I)
                            score_match = re.search(r'(\d+)/10', container_text)
                            
                            if level_match and score_match:
                                level = level_match.group(1).title()
                                score = score_match.group(1)
                                property_data[risk_key] = f"{level} ({score}/10)"
                                break
                        except:
                            continue
                            
                    if property_data[risk_key] != 'N/A':
                        continue
                        
                except:
                    pass
                    
        except:
            pass
    
    def extract_market_data_detailed(self, property_data):
        try:
            history_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'Price history') or contains(text(), 'Sold') or contains(text(), 'Listed')]")
            
            history = []
            for element in history_elements:
                try:
                    container = element.find_element(By.XPATH, "./..")
                    container_text = container.text
                    
                    history_matches = re.findall(r'(\d{1,2}/\d{1,2}/\d{4})\s+([A-Za-z\s]+)\s+(\$[\d,]+)', container_text)
                    for match in history_matches:
                        history.append({
                            'date': match[0],
                            'event': match[1].strip(),
                            'price': match[2]
                        })
                    
                    if len(history) >= 5:
                        break
                        
                except:
                    continue
            
            property_data['property_history'] = history
            
        except Exception as e:
            pass
    
    def extract_nearby_cities(self, property_data):
        try:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(1.5, 2.5))
            
            property_data['nearby_cities'] = []
            property_data['region'] = 'N/A'
            
            page_source = self.driver.page_source
            
            region_patterns = [
                r'Region:\s*([^<\n•]+)',
                r'Region[:\s]+([^<\n•]+)',
                r'Location[^<]*Region[:\s]*([^<\n•]+)'
            ]
            
            for pattern in region_patterns:
                region_match = re.search(pattern, page_source, re.I)
                if region_match:
                    region_text = region_match.group(1).strip()
                    if region_text and len(region_text) > 2:
                        property_data['region'] = region_text
                        break
            
            if property_data['region'] == 'N/A':
                try:
                    location_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'Location')]")
                    for location_elem in location_elements:
                        try:
                            for xpath in [".//..", "./../..", "./../../../.."]:
                                container = location_elem.find_element(By.XPATH, xpath)
                                container_text = container.text
                                
                                region_match = re.search(r'Region:\s*([^•\n]+)', container_text, re.I)
                                if region_match:
                                    property_data['region'] = region_match.group(1).strip()
                                    break
                            
                            if property_data['region'] != 'N/A':
                                break
                        except:
                            continue
                except:
                    pass
            
            nearby_cities_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'Nearby cities')]")
            
            if nearby_cities_elements:
                self.driver.execute_script("arguments[0].scrollIntoView();", nearby_cities_elements[0])
                time.sleep(2)
                
                container = nearby_cities_elements[0].find_element(By.XPATH, "./../..")
                city_links = container.find_elements(By.XPATH, ".//a[contains(text(), 'Real estate')]")
                
                cities = []
                for link in city_links[:5]:
                    try:
                        city_text = link.text.strip()
                        city_name = city_text.replace(' Real estate', '').strip()
                        if city_name and city_name not in cities:
                            cities.append(city_name)
                    except:
                        continue
                
                property_data['nearby_cities'] = cities
            
            if not property_data['nearby_cities']:
                nearby_section = re.search(r'Nearby cities(.*?)(?=<div|</section|</footer)', page_source, re.I | re.DOTALL)
                
                if nearby_section:
                    section_text = nearby_section.group(1)
                    city_matches = re.findall(r'([A-Za-z\s]+?)\s+Real estate', section_text)
                    
                    cities = []
                    for city in city_matches[:5]:
                        clean_city = city.strip()
                        if clean_city and len(clean_city) > 2:
                            cities.append(clean_city)
                    
                    property_data['nearby_cities'] = cities
                    
        except:
            pass
    
    def save_all_properties(self, filename_prefix="massachusetts_properties"):
        """Save all scraped properties to JSON and CSV"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if self.all_properties_data:
            # Save to JSON
            json_filename = f"{filename_prefix}_{timestamp}.json"
            with open(json_filename, 'w') as f:
                json.dump(self.all_properties_data, f, indent=2)
            
            # Save to CSV
            csv_filename = f"{filename_prefix}_{timestamp}.csv"
            flattened_data = []
            for property_data in self.all_properties_data:
                flattened_data.append(self.flatten_property_data(property_data))
            
            df = pd.DataFrame(flattened_data)
            df.to_csv(csv_filename, index=False)
            
            print(f"\n📁 All properties saved:")
            print(f"   • {json_filename} (structured)")
            print(f"   • {csv_filename} (flattened)")
            print(f"   • Total properties: {len(self.all_properties_data)}")
            
            return json_filename, csv_filename  # ✅ Return both files
        else:
            print("No properties data to save")
            return None, None
    
    def flatten_property_data(self, data):
        """Flatten nested data for CSV export"""
        flattened = {}
        
        for key, value in data.items():
            if isinstance(value, list):
                flattened[key] = '; '.join(str(item) for item in value)
            elif isinstance(value, dict):
                for nested_key, nested_value in value.items():
                    if isinstance(nested_value, dict):
                        for deep_key, deep_value in nested_value.items():
                            flattened[f"{key}_{nested_key}_{deep_key}"] = deep_value
                    else:
                        flattened[f"{key}_{nested_key}"] = nested_value
            else:
                flattened[key] = value
        
        return flattened 