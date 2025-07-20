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
from zillow import MultiPropertyZillowScraper

if __name__ == "__main__":
    
    print("="*80)
    print("QUEUE-BASED MASSACHUSETTS ZILLOW SCRAPER - 10,000 PROPERTIES TARGET")
    print("="*80)
    
    # Get terminal/queue ID from environment variable (1-5)
    queue_id = int(os.getenv('QUEUE_ID', '1'))
    headless = os.getenv('HEADLESS', 'false').lower() == 'true'
    output_base_dir = os.getenv('OUTPUT_DIR', 'data')
    
    # UPDATED COUNTY QUEUES WITH PROPER URLS
    city_queues = {
        1: [("middlesex-county-ma", 800, "https://www.zillow.com/middlesex-county-ma/?searchQueryState=%7B\"pagination\"%3A%7B%7D%2C\"isMapVisible\"%3Atrue%2C\"mapBounds\"%3A%7B\"west\"%3A-72.11103208691407%2C\"east\"%3A-70.80777891308594%2C\"south\"%3A42.013380592861715%2C\"north\"%3A42.87776941681747%7D%2C\"regionSelection\"%3A%5B%7B\"regionId\"%3A2801%2C\"regionType\"%3A4%7D%5D%2C\"filterState\"%3A%7B\"sort\"%3A%7B\"value\"%3A\"globalrelevanceex\"%7D%7D%2C\"isListVisible\"%3Atrue%7D")],
        
        2: [("suffolk-county-ma", 800, "https://www.zillow.com/suffolk-county-ma/?searchQueryState=%7B\"pagination\"%3A%7B%7D%2C\"isMapVisible\"%3Atrue%2C\"mapBounds\"%3A%7B\"west\"%3A-71.40629566459278%2C\"east\"%3A-70.75466907767871%2C\"south\"%3A42.07344501265707%2C\"north\"%3A42.50671880749019%7D%2C\"regionSelection\"%3A%5B%7B\"regionId\"%3A2045%2C\"regionType\"%3A4%7D%5D%2C\"filterState\"%3A%7B\"sort\"%3A%7B\"value\"%3A\"globalrelevanceex\"%7D%7D%2C\"isListVisible\"%3Atrue%2C\"mapZoom\"%3A11%7D")],
        
        3: [("worcester-county-ma", 800, "https://www.zillow.com/worcester-county-ma/?searchQueryState=%7B\"pagination\"%3A%7B%7D%2C\"isMapVisible\"%3Atrue%2C\"mapBounds\"%3A%7B\"west\"%3A-72.54849508691407%2C\"east\"%3A-71.24524191308595%2C\"south\"%3A41.93157488769784%2C\"north\"%3A42.797083876551426%7D%2C\"regionSelection\"%3A%5B%7B\"regionId\"%3A2879%2C\"regionType\"%3A4%7D%5D%2C\"filterState\"%3A%7B\"sort\"%3A%7B\"value\"%3A\"globalrelevanceex\"%7D%7D%2C\"isListVisible\"%3Atrue%7D")],
        
        4: [("plymouth-county-ma", 800, "https://www.zillow.com/plymouth-county-ma/?searchQueryState=%7B\"pagination\"%3A%7B%7D%2C\"isMapVisible\"%3Atrue%2C\"mapBounds\"%3A%7B\"west\"%3A-71.35306908691408%2C\"east\"%3A-70.04981591308595%2C\"south\"%3A41.53481252077754%2C\"north\"%3A42.405729812422514%7D%2C\"regionSelection\"%3A%5B%7B\"regionId\"%3A2482%2C\"regionType\"%3A4%7D%5D%2C\"filterState\"%3A%7B\"sort\"%3A%7B\"value\"%3A\"globalrelevanceex\"%7D%7D%2C\"isListVisible\"%3Atrue%7D")],
        
        5: [("norfolk-county-ma", 800, "https://www.zillow.com/norfolk-county-ma/?searchQueryState=%7B\"pagination\"%3A%7B%7D%2C\"isMapVisible\"%3Atrue%2C\"mapBounds\"%3A%7B\"west\"%3A-71.74104658691407%2C\"east\"%3A-70.43779341308594%2C\"south\"%3A41.74482944132208%2C\"north\"%3A42.61288905433203%7D%2C\"regionSelection\"%3A%5B%7B\"regionId\"%3A1948%2C\"regionType\"%3A4%7D%5D%2C\"filterState\"%3A%7B\"sort\"%3A%7B\"value\"%3A\"globalrelevanceex\"%7D%7D%2C\"isListVisible\"%3Atrue%7D")],
        
        6: [("essex-county-ma", 800, "https://www.zillow.com/essex-county-ma/?searchQueryState=%7B\"pagination\"%3A%7B%7D%2C\"isMapVisible\"%3Atrue%2C\"mapBounds\"%3A%7B\"west\"%3A-71.53074958691406%2C\"east\"%3A-70.22749641308593%2C\"south\"%3A42.20357091004269%2C\"north\"%3A43.065348791989685%7D%2C\"regionSelection\"%3A%5B%7B\"regionId\"%3A503%2C\"regionType\"%3A4%7D%5D%2C\"filterState\"%3A%7B\"sort\"%3A%7B\"value\"%3A\"globalrelevanceex\"%7D%7D%2C\"isListVisible\"%3Atrue%7D")],
        
        7: [("bristol-county-ma", 800, "https://www.zillow.com/bristol-county-ma/?searchQueryState=%7B\"pagination\"%3A%7B%7D%2C\"isMapVisible\"%3Atrue%2C\"mapBounds\"%3A%7B\"west\"%3A-71.71868858691406%2C\"east\"%3A-70.41543541308593%2C\"south\"%3A41.31870167022555%2C\"north\"%3A42.19254759138681%7D%2C\"regionSelection\"%3A%5B%7B\"regionId\"%3A1558%2C\"regionType\"%3A4%7D%5D%2C\"filterState\"%3A%7B\"sort\"%3A%7B\"value\"%3A\"globalrelevanceex\"%7D%7D%2C\"isListVisible\"%3Atrue%7D")],
        
        8: [("berkshire-county-ma", 400, "https://www.zillow.com/berkshire-county-ma/?searchQueryState=%7B\"pagination\"%3A%7B%7D%2C\"isMapVisible\"%3Atrue%2C\"mapBounds\"%3A%7B\"west\"%3A-73.88007408691406%2C\"east\"%3A-72.57682091308594%2C\"south\"%3A41.95984174613806%2C\"north\"%3A42.82496387242598%7D%2C\"regionSelection\"%3A%5B%7B\"regionId\"%3A2628%2C\"regionType\"%3A4%7D%5D%2C\"filterState\"%3A%7B\"sort\"%3A%7B\"value\"%3A\"globalrelevanceex\"%7D%7D%2C\"isListVisible\"%3Atrue%7D"), 
            ("hampden-county-ma", 500, "https://www.zillow.com/hampden-county-ma/?searchQueryState=%7B\"pagination\"%3A%7B%7D%2C\"isMapVisible\"%3Atrue%2C\"mapBounds\"%3A%7B\"west\"%3A-73.25658508691407%2C\"east\"%3A-71.95333191308595%2C\"south\"%3A41.73538473375339%2C\"north\"%3A42.603573105579486%7D%2C\"regionSelection\"%3A%5B%7B\"regionId\"%3A1746%2C\"regionType\"%3A4%7D%5D%2C\"filterState\"%3A%7B\"sort\"%3A%7B\"value\"%3A\"globalrelevanceex\"%7D%7D%2C\"isListVisible\"%3Atrue%7D")]
    }
    # Grand Total: ~6,100 properties (leaving room for actual availability)
    
    # Get the queue for this terminal
    my_queue = city_queues.get(queue_id, city_queues[1])
    
    # Calculate expected total for this queue
    expected_total = sum(count for city, count, _ in my_queue)
    
    print(f"Configuration:")
    print(f"  • Queue ID: {queue_id}")
    print(f"  • Cities in queue: {len(my_queue)}")
    print(f"  • Expected properties: {expected_total}")
    print(f"  • Headless mode: {headless}")
    print(f"  • Output base directory: {output_base_dir}")
    print("-" * 60)
    print(f"Queue {queue_id} cities:")
    for city, count, _ in my_queue: 
        print(f"  • {city}: {count} properties")
    print("="*80)
    
    # Create base output directory
    os.makedirs(output_base_dir, exist_ok=True)
    
    # Initialize scraper once for all cities
    scraper = MultiPropertyZillowScraper(headless=headless)
    
    # Process each city in the queue
    total_properties_scraped = 0
    cities_completed = 0
    cities_failed = 0
    
    for city_index, (city, max_properties_this_city, search_url) in enumerate(my_queue, 1):
        print(f"\n" + "🏙️ " * 20)
        print(f"QUEUE {queue_id} - CITY {city_index}/{len(my_queue)}: {city}")
        print(f"Target: {max_properties_this_city} properties")
        print(f"Using optimized search URL: {search_url}")
        print(f"🏙️ " * 20)
        
        try:
            # Create city-specific output directory with clean name
            city_dir_name = city.replace('-MA', '').replace('-', '_').lower()
            city_output_dir = os.path.join(output_base_dir, f"queue_{queue_id}", city_dir_name)
            os.makedirs(city_output_dir, exist_ok=True)
            
            # Save current directory
            original_dir = os.getcwd()
            
            try:
                # Change to city output directory
                os.chdir(city_output_dir)
                
                # Scrape properties for this city with the PROPER URL
                print(f"\n🚀 Starting to scrape {max_properties_this_city} properties from {city}...")
                all_properties = scraper.scrape_multiple_properties(search_url, max_properties=max_properties_this_city)
                
                # Rest of the code stays the same...
                
                # Save data for this city with unique naming
                if all_properties:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    # Create unique filename with city and queue info
                    safe_city_name = city.replace('-MA', '').replace('-', '_').lower()
                    filename_prefix = f"zillow_q{queue_id}_{safe_city_name}_{max_properties_this_city}props_{timestamp}"
                    
                    json_file, csv_file = scraper.save_all_properties(filename_prefix=filename_prefix)
                    
                    # Create city summary
                    city_summary = {
                        "queue_id": queue_id,
                        "city": city,
                        "target_properties": max_properties_this_city,
                        "actual_properties": len(all_properties),
                        "city_index": city_index,
                        "timestamp": timestamp,
                        "json_file": json_file,
                        "csv_file": csv_file,
                        "output_directory": city_output_dir
                    }
                    
                    summary_file = f"summary_q{queue_id}_{safe_city_name}_{max_properties_this_city}props_{timestamp}.json"
                    with open(summary_file, 'w') as f:
                        json.dump(city_summary, f, indent=2)
                    
                    print(f"\n✅ {city} COMPLETED!")
                    print(f"   🎯 Target: {max_properties_this_city} properties")
                    print(f"   ✓ Actual: {len(all_properties)} properties")
                    print(f"   📁 Data saved to: {city_output_dir}")
                    print(f"   📄 JSON: {json_file}")
                    print(f"   📊 CSV: {csv_file}")
                    
                    total_properties_scraped += len(all_properties)
                    cities_completed += 1
                    
                    # Clear the scraper's data for next city
                    scraper.all_properties_data = []
                    
                else:
                    print(f"\n❌ {city} FAILED - No properties scraped")
                    cities_failed += 1
                    
            finally:
                # Always return to original directory
                os.chdir(original_dir)
                
        except Exception as e:
            print(f"\n❌ ERROR in {city}: {e}")
            import traceback
            traceback.print_exc()
            cities_failed += 1
            
            # Try to save partial data if any
            if scraper.all_properties_data:
                try:
                    os.chdir(city_output_dir)
                    scraper.save_all_properties(filename_prefix=f"zillow_{city}_error_partial")
                    scraper.all_properties_data = []  # Clear for next city
                except:
                    pass
                finally:
                    os.chdir(original_dir)
        
        # Progress update
        remaining_cities = len(my_queue) - city_index
        progress_percentage = (total_properties_scraped / expected_total) * 100
        
        print(f"\n📊 QUEUE {queue_id} PROGRESS:")
        print(f"   • Completed: {cities_completed}/{len(my_queue)} cities")
        print(f"   • Failed: {cities_failed}/{len(my_queue)} cities")
        print(f"   • Remaining: {remaining_cities} cities")
        print(f"   • Total properties: {total_properties_scraped}/{expected_total} ({progress_percentage:.1f}%)")
        
        if remaining_cities > 0:
            next_city, next_target, _ = my_queue[city_index]
            print(f"   • Next city: {next_city} (target: {next_target} properties)")
            print(f"\n⏳ Waiting 60 seconds before next city...")
            time.sleep(60)  # Longer pause between cities for high-volume scraping
    
    # Final cleanup
    try:
        scraper.driver.quit()
    except:
        pass
    
    # Final summary
    print(f"\n" + "🎉" * 20)
    print(f"QUEUE {queue_id} COMPLETED!")
    print(f"🎉" * 20)
    print(f"Final Results:")
    print(f"   ✅ Cities completed: {cities_completed}/{len(my_queue)}")
    print(f"   ❌ Cities failed: {cities_failed}/{len(my_queue)}")
    print(f"   🎯 Target properties: {expected_total}")
    print(f"   📊 Actual properties: {total_properties_scraped}")
    print(f"   📈 Success rate: {(total_properties_scraped/expected_total)*100:.1f}%")
    print(f"   📁 Data saved in: {output_base_dir}/queue_{queue_id}/")
    
    # Create overall queue summary
    try:
        os.chdir(output_base_dir)
        queue_summary = {
            "queue_id": queue_id,
            "total_cities": len(my_queue),
            "cities_completed": cities_completed,
            "cities_failed": cities_failed,
            "target_properties": expected_total,
            "actual_properties": total_properties_scraped,
            "success_rate": (total_properties_scraped/expected_total)*100,
            "cities_processed": [city for city, _, _ in my_queue[:cities_completed + cities_failed]],
            "completion_time": datetime.now().isoformat()
        }
        
        with open(f"queue_{queue_id}_final_summary.json", 'w') as f:
            json.dump(queue_summary, f, indent=2)
            
        print(f"   📋 Queue summary saved: queue_{queue_id}_final_summary.json")
        
    except:
        pass
    
    print("\n" + "="*80)
    print("QUEUE PROCESSING COMPLETED")
    print("="*80)