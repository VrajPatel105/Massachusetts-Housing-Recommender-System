import time
import json
import random
from datetime import datetime
from city_queues import city_queues
import os
from zillow import MultiPropertyZillowScraper  

def smart_sleep(sleep_type):
    """Smart randomized delays"""
    sleep_ranges = {
        'between_properties': (1.5, 3.0),
        'between_cities': (45, 75),        # Randomized city breaks
        'after_error': (10, 20),           # Error recovery
        'navigation': (2, 4)               # General navigation
    }
    
    delay = random.uniform(*sleep_ranges.get(sleep_type, (2, 4)))
    return delay

if __name__ == "__main__":
    
    print("="*80)
    print("QUEUE-BASED MASSACHUSETTS ZILLOW SCRAPER - 10,000 PROPERTIES TARGET")
    print("="*80)
    
    # Get terminal/queue ID from environment variable (1-8)
    queue_id = int(os.getenv('QUEUE_ID', '1'))
    headless = os.getenv('HEADLESS', 'false').lower() == 'true'
    output_base_dir = os.getenv('OUTPUT_DIR', 'data')
    
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
    
    # Create base output directory - FIX 2: Use absolute paths
    base_dir = os.path.abspath(output_base_dir)
    os.makedirs(base_dir, exist_ok=True)
    
    # Initialize scraper once for all cities
    try:
        scraper = MultiPropertyZillowScraper(headless=headless)
    except Exception as e:
        print(f"Failed to initialize scraper: {e}")
        exit(1)
    
    # Process each city in the queue
    total_properties_scraped = 0
    cities_completed = 0
    cities_failed = 0
    
    for city_index, (city, max_properties_this_city, search_url) in enumerate(my_queue, 1):
        print(f"\n" + "🏙️ " * 20)
        print(f"QUEUE {queue_id} - CITY {city_index}/{len(my_queue)}: {city}")
        print(f"Target: {max_properties_this_city} properties")
        print(f"Using optimized search URL: {search_url[:60]}...")
        print(f"🏙️ " * 20)
        
        try:
            
            city_dir_name = city.replace('-ma', '').replace('-', '_').lower()
            city_output_dir = os.path.join(base_dir, f"queue_{queue_id}", city_dir_name)
            os.makedirs(city_output_dir, exist_ok=True)
            
            
            print(f"📁 Output directory: {city_output_dir}")
            
            # Scrape properties for this city
            print(f"\n🚀 Starting to scrape {max_properties_this_city} properties from {city}...")
            all_properties = scraper.scrape_multiple_properties(search_url, max_properties=max_properties_this_city)
            
            # Save data for this city with unique naming
            if all_properties:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_city_name = city.replace('-ma', '').replace('-', '_').lower()
                filename_prefix = f"zillow_q{queue_id}_{safe_city_name}_{max_properties_this_city}props_{timestamp}"
                
                
                original_cwd = os.getcwd()
                try:
                    os.chdir(city_output_dir)
                    json_file, csv_file = scraper.save_all_properties(filename_prefix=filename_prefix)
                finally:
                    os.chdir(original_cwd)
                
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
                    "output_directory": city_output_dir,
                    "success_rate": (len(all_properties) / max_properties_this_city) * 100
                }
                
                
                summary_file = os.path.join(city_output_dir, f"summary_q{queue_id}_{safe_city_name}_{timestamp}.json")
                with open(summary_file, 'w') as f:
                    json.dump(city_summary, f, indent=2)
                
                print(f"\n{city} COMPLETED!")
                print(f"  -> Target: {max_properties_this_city} properties")
                print(f"   ✓ Actual: {len(all_properties)} properties")
                print(f"    Success: {(len(all_properties)/max_properties_this_city)*100:.1f}%")
                print(f"   📁 Data saved to: {city_output_dir}")
                print(f"   📄 Files: {json_file}, {csv_file}")
                
                total_properties_scraped += len(all_properties)
                cities_completed += 1
                
                # Clear the scraper's data for next city
                scraper.all_properties_data = []
                scraper.scraped_urls = set() 
                
            else:
                print(f"\n{city} FAILED - No properties scraped")
                cities_failed += 1
                
        except Exception as e:
            print(f"\nERROR in {city}: {e}")
            import traceback
            traceback.print_exc()
            cities_failed += 1
            
            # Try to save partial data if any
            if hasattr(scraper, 'all_properties_data') and scraper.all_properties_data:
                try:
                    original_cwd = os.getcwd()
                    os.chdir(city_output_dir)
                    scraper.save_all_properties(filename_prefix=f"zillow_{city}_error_partial")
                    os.chdir(original_cwd)
                    scraper.all_properties_data = []
                except Exception as save_error:
                    print(f" Could not save partial data: {save_error}")
            
            # FIX 7: Longer delay after errors
            error_delay = smart_sleep('after_error')
            print(f"⏳ Error recovery delay: {error_delay:.1f} seconds...")
            time.sleep(error_delay)
        
        # Progress update
        remaining_cities = len(my_queue) - city_index
        progress_percentage = (total_properties_scraped / expected_total) * 100 if expected_total > 0 else 0
        
        print(f"\n QUEUE {queue_id} PROGRESS:")
        print(f"   • Completed: {cities_completed}/{len(my_queue)} cities")
        print(f"   • Failed: {cities_failed}/{len(my_queue)} cities")
        print(f"   • Remaining: {remaining_cities} cities")
        print(f"   • Total properties: {total_properties_scraped}/{expected_total} ({progress_percentage:.1f}%)")
        
        # FIX 8: Smart randomized delays between cities
        if remaining_cities > 0:
            next_city, next_target, _ = my_queue[city_index]
            print(f"   • Next city: {next_city} (target: {next_target} properties)")
            
            city_delay = smart_sleep('between_cities')
            print(f"\n City break: {city_delay:.1f} seconds before {next_city}...")
            time.sleep(city_delay)
    
    # Final cleanup
    try:
        scraper.driver.quit()
        print("Browser closed successfully")
    except Exception as e:
        print(f" Browser cleanup warning: {e}")
    
    # Final summary
    print(f"\n" + "-_-_-" * 20)
    print(f"QUEUE {queue_id} COMPLETED!")
    print(f"Final Results:")
    print(f"  Cities completed: {cities_completed}/{len(my_queue)}")
    print(f" Cities failed: {cities_failed}/{len(my_queue)}")
    print(f"  Target properties: {expected_total}")
    print(f"  Actual properties: {total_properties_scraped}")
    
    success_rate = (total_properties_scraped/expected_total)*100 if expected_total > 0 else 0
    print(f" Success rate: {success_rate:.1f}%")
    print(f" Data saved in: {base_dir}/queue_{queue_id}/")
    
    # Create overall queue summary
    try:
        queue_summary = {
            "queue_id": queue_id,
            "total_cities": len(my_queue),
            "cities_completed": cities_completed,
            "cities_failed": cities_failed,
            "target_properties": expected_total,
            "actual_properties": total_properties_scraped,
            "success_rate": success_rate,
            "cities_list": [{"name": city, "target": count, "completed": i < cities_completed} 
                           for i, (city, count, _) in enumerate(my_queue)],
            "completion_time": datetime.now().isoformat(),
            "total_runtime_minutes": "calculated_at_runtime"
        }
        
        summary_path = os.path.join(base_dir, f"queue_{queue_id}_final_summary.json")
        with open(summary_path, 'w') as f:
            json.dump(queue_summary, f, indent=2)
            
        print(f"Queue summary: {summary_path}")
        
    except Exception as e:
        print(f"Could not save queue summary: {e}")
    
    print("\n" + "="*80)
    print("QUEUE PROCESSING COMPLETED - CHECK OUTPUT DIRECTORY FOR DATA")
    print("="*80)