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

class MultiPropertyZillowScraper:
    def __init__(self, headless=False):
        self.all_properties_data = []
        self.scraped_urls = set()
        self.last_scraped_url = None  # Track last scraped URL to avoid duplicates
        self.setup_driver(headless)
        
    def setup_driver(self, headless):
        try:
            
            
            options = uc.ChromeOptions()
            if headless:
                options.add_argument("--headless=new")
            
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            
            # Normal window - no custom sizing
            self.driver = uc.Chrome(options=options, version_main=None)
            
        except Exception as e:
            from webdriver_manager.chrome import ChromeDriverManager
            
            options = Options()
            if headless:
                options.add_argument("--headless")
            
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)

    def navigate_to_search_page(self, search_url):
        """Navigate to search page with recovery"""
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                if not self.check_and_recover_driver():
                    continue
                    
                print(f"🔄 Navigating to search page (attempt {attempt + 1})...")
                self.driver.get(search_url)
                time.sleep(3)
                
                # Verify we're on the search page
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="grid-search-results"]/ul'))
                )
                print("✅ Successfully loaded search page")
                return True
                
            except Exception as e:
                print(f"❌ Failed to load search page (attempt {attempt + 1}): {e}")
                if attempt < max_attempts - 1:
                    time.sleep(2)
                    continue
                else:
                    return False
        
        return False

    def check_and_recover_driver(self):
        """Check if driver is still active and recover if needed"""
        try:
            # Simple check to see if driver is responsive
            self.driver.current_url
            return True
        except Exception as e:
            print(f"⚠️ Driver connection lost: {e}")
            print("🔄 Attempting to recover browser session...")
            
            try:
                # Try to quit the existing driver
                self.driver.quit()
            except:
                pass
            
            # Reinitialize the driver
            self.setup_driver(headless=False)
            print("✅ Browser session recovered")
            return True

    def check_driver_health(self):
        """Enhanced driver health check"""
        try:
            # Multiple health checks
            url = self.driver.current_url
            title = self.driver.title
            ready_state = self.driver.execute_script("return document.readyState;")
            
            # Check if we can interact with the page
            self.driver.find_element(By.TAG_NAME, "body")
            
            return True
        except Exception as e:
            print(f"  ⚠️ Driver health check failed: {e}")
            return False

    def dismiss_overlays(self):
        """Dismiss any overlay elements that might block clicks"""
        try:
            # Close any lightbox overlays
            overlay_selectors = [
                '[class*="LightboxMask"]',
                '[role="presentation"][opacity]',
                '.modal-overlay',
                '.overlay',
                '[class*="overlay"]'
            ]
            
            for selector in overlay_selectors:
                try:
                    overlays = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for overlay in overlays:
                        if overlay.is_displayed():
                            # Try clicking the overlay to dismiss it
                            self.driver.execute_script("arguments[0].click();", overlay)
                            time.sleep(0.5)
                            # Or try pressing Escape
                            self.driver.execute_script("document.body.dispatchEvent(new KeyboardEvent('keydown', {key: 'Escape'}));")
                            time.sleep(0.5)
                except:
                    continue
                    
            # Try pressing Escape key to close any modals
            from selenium.webdriver.common.keys import Keys
            try:
                self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                time.sleep(0.5)
            except:
                pass
                
        except Exception as e:
            print(f"  - Error dismissing overlays: {e}")

    def recover_from_click_failures(self, search_url):
        """Recover browser when clicks start failing systematically"""
        try:
            print("🔄 Recovering from systematic click failures...")
            
            # Clear browser state
            self.driver.execute_script("window.stop();")
            self.driver.execute_script("window.localStorage.clear();")
            self.driver.execute_script("window.sessionStorage.clear();")
            
            # Navigate back to search page
            self.driver.get(search_url)
            time.sleep(5)
            
            # Verify we're back on search results
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[1]/ul'))
            )
            
            print("✅ Browser recovery successful")
            return True
            
        except Exception as e:
            print(f"❌ Browser recovery failed: {e}")
            return False

    def scrape_multiple_properties(self, search_url, max_properties=50):
        """FIXED: Stop when target reached"""
        print(f"Starting to scrape {max_properties} properties from search results...")
        
        self.ad_detected = False
        self.driver.get(search_url)
        time.sleep(5)
        
        properties_scraped = 0
        current_page = 1
        consecutive_failures = 0
        
        while properties_scraped < max_properties:
            print(f"\n=== PAGE {current_page} ===")
            
            # Wait for search results
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[1]/ul'))
                )
                print("Search results loaded")
            except:
                print("❌ Search results failed to load")
                break
            
            property_count = self.get_property_count()
            print(f"Found {property_count} properties on page {current_page}")
            
            # Process properties on this page
            property_index = 1
            while property_index <= property_count and properties_scraped < max_properties:
                print(f"\nProcessing property {properties_scraped + 1}/{max_properties} (Page {current_page}, Property {property_index}/{property_count})")
                
                # FIXED: Check if we've reached target BEFORE processing
                if properties_scraped >= max_properties:
                    print(f"✅ Reached target of {max_properties} properties")
                    break
                
                # Try to click property
                success = self.click_and_navigate_to_property(property_index)
                
                if success:
                    # Extract data
                    try:
                        property_data = self.extract_complete_property_data()
                        if property_data:
                            self.all_properties_data.append(property_data)
                            properties_scraped += 1
                            consecutive_failures = 0
                            print(f"✅ Successfully scraped property {properties_scraped}")
                            
                            # FIXED: Check target AFTER scraping
                            if properties_scraped >= max_properties:
                                print(f"✅ Reached target of {max_properties} properties")
                                break
                            
                            # Save checkpoint every 5 properties
                            if properties_scraped % 5 == 0:
                                self.save_progress_checkpoint("current_scrape", properties_scraped)
                        else:
                            consecutive_failures += 1
                    except Exception as e:
                        print(f"❌ Data extraction failed: {e}")
                        consecutive_failures += 1
                    
                    # Return to search
                    if not self.return_to_search_results(search_url, current_page):
                        consecutive_failures += 1
                        if consecutive_failures >= 5:
                            print("❌ Too many failures, stopping")
                            break
                else:
                    # Don't count ad skips as failures
                    if hasattr(self, 'ad_detected') and self.ad_detected:
                        print(f"  📢 Ad detected, continuing to next property...")
                        self.ad_detected = False
                    else:
                        consecutive_failures += 1
                        if consecutive_failures >= 15:
                            print("❌ Too many real failures, moving to next page")
                            consecutive_failures = 0
                            break
                
                property_index += 1
            
            # FIXED: Stop if we reached target
            if properties_scraped >= max_properties:
                print(f"✅ Reached target of {max_properties} properties")
                break
            
            # Move to next page
            if properties_scraped < max_properties:
                try:
                    if self.go_to_next_page():
                        current_page += 1
                        consecutive_failures = 0
                        time.sleep(3)
                    else:
                        print("❌ No more pages")
                        break
                except:
                    print("❌ Page navigation failed")
                    break
        
        print(f"\n🎉 Scraping completed! Total properties: {properties_scraped}")
        return self.all_properties_data

    # ALTERNATIVE: If you want to scroll to stay in position after ads
    def scroll_to_property_position(self, property_index):
        """Scroll to approximate position of property in list"""
        try:
            # Calculate approximate scroll position
            # Each property card is roughly 300-400px tall
            approximate_scroll = (property_index - 1) * 350
            
            # Scroll to that position
            self.driver.execute_script(f"window.scrollTo(0, {approximate_scroll});")
            time.sleep(1)
            
        except Exception as e:
            print(f"  Error scrolling to position: {e}")

    # OPTIONAL: Enhanced version that maintains scroll position
    def click_and_navigate_to_property(self, property_index):
        """FIXED: Simple ad detection that actually works"""
        try:
            print(f"  Processing property at index {property_index}")
            
            # Find the property element
            property_xpath = f'/html/body/div[1]/div/div[2]/div/div/div[1]/div[1]/ul/li[{property_index}]'
            
            try:
                property_element = self.driver.find_element(By.XPATH, property_xpath)
            except:
                print(f"  ⚠️ Property element {property_index} not found")
                return False
            
            # FIXED: Better ad check - look for actual property indicators
            try:
                links = property_element.find_elements(By.XPATH, './/a[@href]')
                valid_links = [link for link in links if link.get_attribute('href') and '/homedetails/' in link.get_attribute('href')]
                
                if not valid_links:
                    # Check if it has property-like content before calling it an ad
                    element_text = property_element.text.lower()
                    
                    # Real properties have these indicators
                    has_price = '$' in element_text
                    has_beds = any(word in element_text for word in ['bed', 'bd', 'bedroom'])
                    has_sqft = 'sqft' in element_text or 'sq ft' in element_text
                    
                    # If it looks like a property but no link, might be a layout issue
                    if has_price or has_beds or has_sqft:
                        print(f"  ⚠️ Element {property_index} looks like property but no valid link - trying anyway")
                        # Don't skip it, continue with navigation attempt
                    else:
                        print(f"  ⚠️ Element {property_index} appears to be an ad - skipping")
                        self.ad_detected = True
                        return False
                        
                if valid_links:
                    property_url = valid_links[0].get_attribute('href')
                else:
                    # No valid link but looks like property - try to find any navigation
                    all_links = property_element.find_elements(By.XPATH, './/a[@href]')
                    if all_links:
                        # Take first link and hope it works
                        property_url = all_links[0].get_attribute('href')
                        print(f"  ⚠️ Using non-standard link: {property_url}")
                    else:
                        print(f"  ❌ No links found at all")
                        return False
                
            except:
                print(f"  ❌ Error checking property links")
                return False
            
            # Check for duplicates
            if property_url in self.scraped_urls:
                print(f"  ⚠️ Duplicate URL detected, skipping: {property_url}")
                return False
            
            print(f"  Found valid property link: {property_url}")
            
            original_url = self.driver.current_url
            
            # Try direct click first
            try:
                print(f"  Trying Direct Click...")
                if valid_links:
                    clickable_element = valid_links[0]
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", clickable_element)
                    time.sleep(0.5)
                    
                    clickable_element.click()
                    time.sleep(2)
                    
                    current_url = self.driver.current_url
                    if current_url != original_url and '/homedetails/' in current_url:
                        print(f"  ✅ Direct Click successful!")
                        self.scraped_urls.add(property_url)
                        return True
                    else:
                        print(f"  ❌ Direct Click - no valid navigation detected")
                else:
                    print(f"  ❌ Direct Click - no clickable element")
                    
            except Exception as e:
                print(f"  ❌ Direct Click failed: {e}")
            
            # ALWAYS try direct navigation if click fails
            try:
                print(f"  Trying Direct Navigation...")
                self.driver.get(property_url)
                time.sleep(3)
                
                current_url = self.driver.current_url
                if '/homedetails/' in current_url:
                    print(f"  ✅ Direct Navigation successful!")
                    self.scraped_urls.add(property_url)
                    return True
                else:
                    print(f"  ❌ Direct Navigation - not on property page")
                    return False
                    
            except Exception as e:
                print(f"  ❌ Direct Navigation failed: {e}")
                return False
            
        except Exception as e:
            print(f"  ❌ Error in click_and_navigate_to_property: {e}")
            return False
    
    def get_property_count(self):
        """Simple property count - just count li elements"""
        try:
            elements = self.driver.find_elements(By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[1]/ul/li')
            return len(elements)
        except Exception as e:
            print(f"Error counting properties: {e}")
            return 0

    def safe_click(self, element, method="click"):
        """Perform click with timeout protection"""
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError("Click operation timed out")
        
        try:
            # Set timeout for click operation (15 seconds)
            if hasattr(signal, 'SIGALRM'):  # Unix systems
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(15)  # 15 second timeout
            
            try:
                if method == "click":
                    element.click()
                elif method == "js_click":
                    self.driver.execute_script("arguments[0].click();", element)
                elif method == "action_chains":
                    ActionChains(self.driver).move_to_element(element).click().perform()
                
                if hasattr(signal, 'SIGALRM'):
                    signal.alarm(0)  # Cancel timeout
                
                return True
                
            except TimeoutError:
                print(f"    {method} timed out after 15s")
                return False
            except Exception as e:
                print(f"    {method} failed: {e}")
                return False
            finally:
                if hasattr(signal, 'SIGALRM'):
                    signal.alarm(0)  # Ensure timeout is cancelled
                    
        except Exception as e:
            print(f"    Timeout setup failed: {e}")
            # Fallback without timeout
            try:
                if method == "click":
                    element.click()
                elif method == "js_click":
                    self.driver.execute_script("arguments[0].click();", element)
                elif method == "action_chains":
                    ActionChains(self.driver).move_to_element(element).click().perform()
                return True
            except Exception as fallback_e:
                print(f"    Fallback {method} failed: {fallback_e}")
                return False

    def safe_navigate(self, url):
        """Navigate with timeout protection"""
        try:
            # Use shorter timeout for navigation
            self.driver.set_page_load_timeout(20)  # 20 second timeout
            self.driver.get(url)
            return True
        except Exception as e:
            print(f"    Navigation failed: {e}")
            return False
        finally:
            # Reset timeout
            try:
                self.driver.set_page_load_timeout(30)  # Reset to default
            except:
                pass

    def is_valid_property_url(self, url):
        """Check if URL is a valid Zillow property page (not an ad)"""
        if not url or not isinstance(url, str):
            return False
        
        # Valid property URL patterns
        valid_patterns = [
            '/homedetails/',
            '/b/',  # Building pages
            '/homes/',
            'zpid'  # Zillow property ID
        ]
        
        # Invalid patterns (ads, external links, etc.)
        invalid_patterns = [
            'mailto:',
            'tel:',
            'javascript:',
            'google.com',
            'facebook.com',
            'twitter.com',
            'instagram.com',
            'youtube.com',
            'trulia.com',
            'rentals.com',
            'apartments.com',
            '/advertise',
            '/partners',
            '/careers',
            'ads.zillow.com',
            'premier-agent',
            'rental-manager',
            'zestimate'
        ]
        
        # Check for invalid patterns first
        for invalid in invalid_patterns:
            if invalid in url.lower():
                return False
        
        # Check for valid patterns
        for valid in valid_patterns:
            if valid in url.lower():
                return True
        
        # If it's a zillow.com URL but doesn't match patterns, might still be valid
        if 'zillow.com' in url.lower():
            return True
        
        return False

    def is_likely_ad_element(self, element):
        """Simple ad detection - only skip obvious ads"""
        try:
            element_text = element.text.lower()
            
            # Only skip if it has obvious ad keywords AND no property indicators
            obvious_ad_keywords = [
                'premier agent',
                'find an agent', 
                'get pre-approved',
                'mortgage calculator',
                'browse homes',
                'see more homes'
            ]
            
            # Property indicators
            property_indicators = [
                '$', 'bed', 'bath', 'sqft', 'sq ft', 'acre', 'lot'
            ]
            
            has_ad_keywords = any(keyword in element_text for keyword in obvious_ad_keywords)
            has_property_indicators = any(indicator in element_text for indicator in property_indicators)
            
            # Only call it an ad if it has ad keywords AND no property indicators
            return has_ad_keywords and not has_property_indicators
            
        except:
            return False  # If can't determine, assume it's not an ad

    def return_to_search_results(self, search_url, target_page):
        """Enhanced return with complete crash recovery"""
        try:
            print("  Going back to search results...")
            
            # Method 1: Quick health check first
            try:
                current_url = self.driver.current_url
                self.driver.title  # Additional health check
                
                # Try browser back if healthy
                self.driver.back()
                time.sleep(2)
                
                # Verify we're back on search results
                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[1]/ul'))
                )
                print("  ✅ Successfully returned via browser back")
                return True
                
            except Exception as back_error:
                print(f"  ❌ Browser back failed: {back_error}")
            
            # Method 2: Direct navigation with health check
            try:
                print("  Trying direct navigation to search URL...")
                
                # Check if driver is still alive
                try:
                    self.driver.current_url
                    driver_alive = True
                except:
                    driver_alive = False
                    print("  ⚠️ Driver appears dead, attempting recovery...")
                
                if not driver_alive:
                    # Browser crashed - restart it
                    try:
                        self.driver.quit()
                    except:
                        pass
                    
                    # Reinitialize driver
                    self.setup_driver(headless=False)
                    print("  🔄 Browser restarted")
                
                # Navigate to search URL
                self.driver.get(search_url)
                time.sleep(5)
                
                # Navigate to correct page if needed
                if target_page > 1:
                    print(f"  Navigating to page {target_page}...")
                    for page_num in range(target_page - 1):
                        try:
                            if not self.go_to_next_page():
                                print(f"    Failed to reach page {target_page}, stopping at page {page_num + 1}")
                                break
                            time.sleep(2)
                        except Exception as page_error:
                            print(f"    Page navigation failed: {page_error}")
                            break
                
                # Verify search results loaded
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[1]/ul'))
                )
                print("  ✅ Successfully returned via direct navigation")
                return True
                
            except Exception as nav_error:
                print(f"  ❌ Direct navigation failed: {nav_error}")
                
                # Method 3: Complete restart as last resort
                try:
                    print("  🚨 Attempting complete browser restart...")
                    
                    try:
                        self.driver.quit()
                    except:
                        pass
                    
                    time.sleep(3)
                    self.setup_driver(headless=False)
                    
                    # Navigate fresh
                    self.driver.get(search_url)
                    time.sleep(8)
                    
                    # Navigate to page (but start from page 1)
                    current_target_page = min(target_page, 1)  # Don't try to go too far
                    for page_num in range(current_target_page - 1):
                        try:
                            if not self.go_to_next_page():
                                break
                            time.sleep(3)
                        except:
                            break
                    
                    # Final verification
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[1]/ul'))
                    )
                    print("  ✅ Successfully recovered via complete restart")
                    return True
                    
                except Exception as restart_error:
                    print(f"  ❌ Complete restart failed: {restart_error}")
                    return False
                
        except Exception as e:
            print(f"  ❌ Error in return_to_search_results: {e}")
            return False

    def go_to_next_page(self):
        """Navigate to next page of search results"""
        try:
            print("🔍 Looking for 'Next page' button...")
            
            # Find next page button
            next_button_xpath = "//a[@title='Next page']"
            
            try:
                next_button = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, next_button_xpath))
                )
            except TimeoutException:
                print("  ❌ Next page button not found")
                return False
            
            # Check if button is disabled (last page)
            if next_button.get_attribute('aria-disabled') == 'true':
                print("  ✅ Reached last page (next button disabled)")
                return False
            
            # Click next page button
            try:
                print("  Clicking next page button...")
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                time.sleep(1)
                self.driver.execute_script("arguments[0].click();", next_button)
                time.sleep(5)
                
                # Verify new page loaded
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[1]/ul'))
                )
                print("  ✅ Successfully navigated to next page")
                return True
                
            except Exception as e:
                print(f"  ❌ Failed to click next page: {e}")
                return False
                
        except Exception as e:
            print(f"❌ Error in go_to_next_page: {e}")
            return False

    def get_property_links_fallback(self):
        """Fallback method using old selectors"""
        try:
            # Your original method as backup
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="grid-search-results"]/ul'))
            )
            
            selectors_to_try = [
                '//*[@id="grid-search-results"]/ul/li//article//a[contains(@href, "/homedetails/")]',
                '//*[@id="grid-search-results"]/ul/li//a[contains(@href, "/homedetails/")]',
                '//*[@id="grid-search-results"]/ul//a[contains(@href, "/homedetails/")]'
            ]
            
            property_links = []
            for selector in selectors_to_try:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    if elements:
                        print(f"Fallback: Found {len(elements)} elements with selector: {selector}")
                        property_links = elements
                        break
                except Exception as e:
                    continue
            
            return property_links
            
        except Exception as e:
            print(f"Error in fallback method: {e}")
            return []
    
    def go_back_to_search(self):
        """Navigate back to search results using multiple strategies"""
        try:
            print("Attempting to return to search results...")
            
            # Strategy 1: Browser back
            try:
                print("  Strategy 1: Using browser back")
                self.driver.back()
                time.sleep(2)
                
                # SCROLL TO LOAD ALL PROPERTIES AGAIN
                self.scroll_to_load_all_properties()
                
                # Wait for search results to appear
                try:
                    WebDriverWait(self.driver, 8).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="grid-search-results"]/ul'))
                    )
                    print("  ✓ Successfully returned to search results via browser back")
                    return True
                except TimeoutException:
                    print("  ✗ Browser back didn't return to search results")
            except Exception as e:
                print(f"  ✗ Browser back failed: {e}")
            
            # Rest of the function stays the same...
            # (Keep all the other fallback strategies)
            
        except Exception as e:
            print(f"Error in go_back_to_search: {e}")
            return False

    def scroll_to_load_all_properties(self):
        """Fast scroll to load all properties via lazy loading"""
        try:
            
            # Get initial count
            initial_count = len(self.driver.find_elements(By.XPATH, '//*[@id="grid-search-results"]/ul//a[contains(@href, "/homedetails/")]'))
            print(f"  📊 Initial properties found: {initial_count}")
            
            # Fast scroll: 2-3 quick scrolls instead of 10 slow ones
            scroll_steps = [0.3, 0.6, 1.0]  # Scroll to 30%, 60%, 100% of page
            
            for i, scroll_position in enumerate(scroll_steps):
                # Quick scroll to position
                self.driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {scroll_position});")
                time.sleep(0.5)  # Short wait for content to load
                
                # Check count after each scroll
                current_count = len(self.driver.find_elements(By.XPATH, '//*[@id="grid-search-results"]/ul//a[contains(@href, "/homedetails/")]'))
            
            # Quick scroll back to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.3)
            
            final_count = len(self.driver.find_elements(By.XPATH, '//*[@id="grid-search-results"]/ul//a[contains(@href, "/homedetails/")]'))
            
            return final_count
            
        except Exception as e:
            print(f"  ❌ Error during scrolling: {e}")
            return 0        

    def go_to_next_page(self):
        """
        Navigate to the next page using a robust selector that finds the button
        by its title, not its position. This is the reliable way to handle pagination.
        """
        try:
            print("🔍 Looking for the 'Next page' button...")

            # This is the robust XPath. It looks for a link (<a>) with the exact title 'Next page'.
            # This works regardless of the button's position on the page.
            next_button_xpath = "//a[@title='Next page']"

            # Use WebDriverWait to handle cases where the page is still loading.
            # We'll wait up to 5 seconds for the button to even exist.
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
            time.sleep(0.5)  # A brief pause after scrolling.

            # Use a JavaScript click, which is often more reliable than a standard .click().
            self.driver.execute_script("arguments[0].click();", next_button)

            # Wait for the next page to load. A good way to confirm this is to
            # wait for the main property list to be present again.
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div[1]/div[1]/ul'))
            )

            print("  ✅ Successfully navigated to the next page.")
            return True

        except TimeoutException:
            # This will happen if the WebDriverWait times out after 5 seconds.
            # It means the "Next page" button was not found at all.
            print("  ❌ 'Next page' button not found. Assuming end of results.")
            return False

        except Exception as e:
            # Catch any other unexpected errors.
            print(f"❌ An unexpected error occurred in go_to_next_page: {e}")
            return False
        
    def save_progress_checkpoint(self, county_name, current_count):
        """Save progress every 50 properties"""
        if len(self.all_properties_data) % 50 == 0 and len(self.all_properties_data) > 0:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            checkpoint_filename = f"checkpoint_{county_name}_{current_count}props_{timestamp}.json"
            
            with open(checkpoint_filename, 'w') as f:
                json.dump(self.all_properties_data, f, indent=2)
            
            print(f"🔄 CHECKPOINT SAVED: {checkpoint_filename} ({len(self.all_properties_data)} properties)")
    
    def extract_complete_property_data(self):
        """Extract all property data from current property page - optimized version"""
        try:
            print("Starting property data extraction...")
            
            property_data = {
                'url': self.driver.current_url,
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
            time.sleep(1)
            
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
            time.sleep(1.5)  # Wait for content to load
            
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
            time.sleep(2)
            
            property_data['flood_risk'] = 'N/A'
            property_data['fire_risk'] = 'N/A'
            property_data['wind_risk'] = 'N/A'
            property_data['air_risk'] = 'N/A'
            property_data['heat_risk'] = 'N/A'
            
            try:
                climate_section = self.driver.find_element(By.XPATH, "//*[contains(text(), 'Climate risks')]")
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", climate_section)
                time.sleep(3)
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
            time.sleep(2)
            
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
            
            return json_filename, csv_filename
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