import logging
from typing import List, Optional
from playwright.sync_api import sync_playwright, Page
from dataclasses import dataclass, asdict
import pandas as pd
import argparse
import platform
import time
import os
import json
import hashlib
from datetime import datetime

@dataclass
class Place:
    name: str = ""
    address: str = ""
    website: str = ""
    phone_number: str = ""
    reviews_count: Optional[int] = None
    reviews_average: Optional[float] = None
    store_shopping: str = "No"
    in_store_pickup: str = "No"
    store_delivery: str = "No"
    place_type: str = ""
    opens_at: str = ""
    introduction: str = ""

@dataclass
class ScrapingCache:
    search_query: str
    output_file: str
    total_target: int
    scraped_count: int
    last_scraped_index: int
    timestamp: str
    cache_id: str

class CacheManager:
    def __init__(self, cache_dir: str = ".scraping_cache"):
        self.cache_dir = cache_dir
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
    
    def generate_cache_id(self, search_query: str, output_file: str) -> str:
        """Generate unique cache ID based on search query and output file"""
        combined = f"{search_query}_{output_file}"
        return hashlib.md5(combined.encode()).hexdigest()
    
    def save_cache(self, search_query: str, output_file: str, total_target: int, 
                   scraped_count: int, last_scraped_index: int) -> str:
        """Save current scraping progress to cache"""
        cache_id = self.generate_cache_id(search_query, output_file)
        cache_data = ScrapingCache(
            search_query=search_query,
            output_file=output_file,
            total_target=total_target,
            scraped_count=scraped_count,
            last_scraped_index=last_scraped_index,
            timestamp=datetime.now().isoformat(),
            cache_id=cache_id
        )
        
        cache_file = os.path.join(self.cache_dir, f"{cache_id}.json")
        with open(cache_file, 'w') as f:
            json.dump(asdict(cache_data), f, indent=2)
        
        logging.info(f"Cache saved: {cache_file}")
        return cache_id
    
    def load_cache(self, search_query: str, output_file: str) -> Optional[ScrapingCache]:
        """Load existing cache for the same search query and output file"""
        cache_id = self.generate_cache_id(search_query, output_file)
        cache_file = os.path.join(self.cache_dir, f"{cache_id}.json")
        
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    cache_data = json.load(f)
                
                # Verify the output file still exists
                if os.path.exists(cache_data['output_file']):
                    logging.info(f"Cache found: {cache_file}")
                    return ScrapingCache(**cache_data)
                else:
                    # Clean up cache if output file no longer exists
                    os.remove(cache_file)
                    logging.info(f"Cache removed (output file missing): {cache_file}")
            except Exception as e:
                logging.warning(f"Failed to load cache {cache_file}: {e}")
        
        return None
    
    def clear_cache(self, search_query: str, output_file: str):
        """Clear cache for completed scraping"""
        cache_id = self.generate_cache_id(search_query, output_file)
        cache_file = os.path.join(self.cache_dir, f"{cache_id}.json")
        
        if os.path.exists(cache_file):
            os.remove(cache_file)
            logging.info(f"Cache cleared: {cache_file}")
    
    def get_existing_data_count(self, output_file: str) -> int:
        """Count existing records in output file using fast line counting"""
        if os.path.exists(output_file):
            try:
                # Ultra-fast line counting without loading entire file into memory
                with open(output_file, 'r', encoding='utf-8') as f:
                    # Count lines and subtract 1 for header (if it exists)
                    line_count = sum(1 for _ in f)
                    # Check if file has header by reading first line
                    f.seek(0)
                    first_line = f.readline().strip()
                    has_header = first_line.startswith('name,') or 'name' in first_line.lower()
                    return max(0, line_count - (1 if has_header else 0))
            except:
                return 0
        return 0
    
    def get_existing_data_count_lightning(self, output_file: str) -> int:
        """Lightning-fast file size estimation using file seek operations"""
        if not os.path.exists(output_file):
            return 0
        try:
            with open(output_file, 'rb') as f:
                # Count newlines using binary search for maximum speed
                f.seek(0, 2)  # Seek to end
                file_size = f.tell()
                if file_size == 0:
                    return 0
                
                # Sample-based line counting for very large files
                if file_size > 1024 * 1024:  # > 1MB
                    # Read a 4KB sample from the middle
                    f.seek(file_size // 2)
                    sample = f.read(4096)
                    if sample:
                        sample_lines = sample.count(b'\n')
                        if sample_lines > 0:
                            estimated_lines = (file_size * sample_lines) // len(sample)
                            return max(0, estimated_lines - 1)  # Subtract header
                
                # For smaller files, do actual count but faster
                f.seek(0)
                line_count = sum(1 for _ in f)
                return max(0, line_count - 1)  # Subtract header
        except:
            return 0

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )

def check_end_of_list_reached(page: Page) -> bool:
    """Check if we've reached the end of Google Maps results list"""
    try:
        # Comprehensive list of end-of-list messages in multiple languages
        end_messages = [
            # English variations
            "You've reached the end of the list",
            "You've reached the end of the list.",
            "You have reached the end of the list",
            "reached the end of the list",
            "end of the list",
            "No more results",
            "No more places",
            "That's all",
            "End of results",
            "No additional results",
            "All results shown",
            
            # Common variations
            "reached the end",
            "end of list",
            "no more",
            "all done",
            "complete list"
        ]
        
        # Check for text-based messages
        for message in end_messages:
            try:
                # Case-insensitive text search
                if page.locator(f'text="{message}"').count() > 0:
                    return True
                # Partial text search
                if page.locator(f'//*[contains(text(), "{message.lower()}")]').count() > 0:
                    return True
            except:
                continue
        
        # Check for specific Google Maps end-of-list elements
        end_selectors = [
            # Google Maps specific selectors
            '//div[contains(@class, "HlvSq")]//text()[contains(., "end")]',
            '//div[contains(@class, "section-loading")]//text()[contains(., "end")]',
            '//div[contains(@class, "section-no-result")]',
            '//span[contains(text(), "No more results")]',
            "//div[contains(text(), \"You've reached\")]",
            '//div[contains(@aria-label, "end")]',
            
            # Generic end indicators
            '//*[contains(@class, "end-of-results")]',
            '//*[contains(@class, "no-more-results")]',
            '//*[contains(@class, "list-end")]',
            '//*[contains(@data-value, "end")]'
        ]
        
        for selector in end_selectors:
            try:
                if page.locator(selector).count() > 0:
                    return True
            except:
                continue
        
        # Check page content for end indicators
        try:
            page_content = page.content()
            if page_content:
                content_lower = page_content.lower()
                end_indicators = [
                    "reached the end",
                    "end of the list",
                    "no more results",
                    "you've reached"
                ]
                
                for indicator in end_indicators:
                    if indicator in content_lower:
                        return True
        except:
            pass
        
        return False
        
    except Exception as e:
        logging.debug(f"Error checking end of list: {e}")
        return False

def extract_text(page: Page, xpath: str) -> str:
    """Optimized text extraction with faster checking"""
    try:
        locator = page.locator(xpath)
        # Use first() to avoid count() - much faster
        if locator.first.is_visible(timeout=500):
            return locator.first.inner_text(timeout=1000)
    except Exception:
        # No logging for performance - just return empty
        pass
    return ""

def extract_place(page: Page) -> Place:
    # XPaths
    name_xpath = '//div[@class="TIHn2 "]//h1[@class="DUwDvf lfPIob"]'
    address_xpath = '//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]'
    website_xpath = '//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]'
    phone_number_xpath = '//button[contains(@data-item-id, "phone:tel:")]//div[contains(@class, "fontBodyMedium")]'
    reviews_count_xpath = '//div[@class="TIHn2 "]//div[@class="fontBodyMedium dmRWX"]//div//span//span//span[@aria-label]'
    reviews_average_xpath = '//div[@class="TIHn2 "]//div[@class="fontBodyMedium dmRWX"]//div//span[@aria-hidden]'
    info1 = '//div[@class="LTs0Rc"][1]'
    info2 = '//div[@class="LTs0Rc"][2]'
    info3 = '//div[@class="LTs0Rc"][3]'
    opens_at_xpath = '//button[contains(@data-item-id, "oh")]//div[contains(@class, "fontBodyMedium")]'
    opens_at_xpath2 = '//div[@class="MkV9"]//span[@class="ZDu9vd"]//span[2]'
    place_type_xpath = '//div[@class="LBgpqf"]//button[@class="DkEaL "]'
    intro_xpath = '//div[@class="WeS02d fontBodyMedium"]//div[@class="PYvSYb "]'

    place = Place()
    place.name = extract_text(page, name_xpath)
    place.address = extract_text(page, address_xpath)
    place.website = extract_text(page, website_xpath)
    place.phone_number = extract_text(page, phone_number_xpath)
    place.place_type = extract_text(page, place_type_xpath)
    place.introduction = extract_text(page, intro_xpath) or "None Found"

    # Reviews Count - Optimized processing
    reviews_count_raw = extract_text(page, reviews_count_xpath)
    if reviews_count_raw:
        try:
            # Faster string processing - chained replaces
            clean_count = reviews_count_raw.replace('\xa0', '').replace('(','').replace(')','').replace(',','')
            place.reviews_count = int(clean_count)
        except:
            pass  # Skip logging for speed
            
    # Reviews Average - Optimized processing  
    reviews_avg_raw = extract_text(page, reviews_average_xpath)
    if reviews_avg_raw:
        try:
            clean_avg = reviews_avg_raw.replace(' ','').replace(',','.')
            place.reviews_average = float(clean_avg)
        except:
            pass  # Skip logging for speed
            
    # Store Info - Optimized loop processing
    for info_xpath in [info1, info2, info3]:
        info_raw = extract_text(page, info_xpath)
        if info_raw and '·' in info_raw:
            # Direct split and check - faster than temp variable
            check = info_raw.split('·')[1].replace("\n", "").lower()
            if 'shop' in check:
                place.store_shopping = "Yes"
            if 'pickup' in check:
                place.in_store_pickup = "Yes"
            if 'delivery' in check:
                place.store_delivery = "Yes"
    # Opens At - Optimized processing
    opens_at_raw = extract_text(page, opens_at_xpath)
    if opens_at_raw and '⋅' in opens_at_raw:
        place.opens_at = opens_at_raw.split('⋅')[1].replace("\u202f","")
    elif opens_at_raw:
        place.opens_at = opens_at_raw.replace("\u202f","")
    else:
        # Try alternative xpath only if first one failed
        opens_at2_raw = extract_text(page, opens_at_xpath2)
        if opens_at2_raw and '⋅' in opens_at2_raw:
            place.opens_at = opens_at2_raw.split('⋅')[1].replace("\u202f","")
        elif opens_at2_raw:
            place.opens_at = opens_at2_raw.replace("\u202f","")
    return place

def scrape_places(search_for: str, total: int, output_path: str = "result.csv", ultra_fast_append: bool = False) -> List[Place]:
    setup_logging()
    cache_manager = CacheManager()
    places: List[Place] = []
    
    # Detect if we're in append mode by checking if file exists
    is_append_mode = os.path.exists(output_path) or ultra_fast_append
    
    # Check for existing cache
    cache = cache_manager.load_cache(search_for, output_path)
    start_index = 0
    
    # Use lightning-fast counting for ultra-fast mode
    if ultra_fast_append:
        existing_count = cache_manager.get_existing_data_count_lightning(output_path)
    else:
        existing_count = cache_manager.get_existing_data_count(output_path)
    
    if cache and existing_count >= cache.scraped_count:
        logging.info(f"🔄 RESUMING SCRAPING from cache...")
        logging.info(f"   Search: {search_for}")
        logging.info(f"   Output: {output_path}")
        logging.info(f"   Previous progress: {cache.scraped_count}/{cache.total_target}")
        logging.info(f"   Last index: {cache.last_scraped_index}")
        start_index = cache.last_scraped_index + 1
        is_append_mode = True  # Force append mode when resuming
        
        if cache.scraped_count >= total:
            logging.info("✅ Scraping already completed according to cache!")
            return []
    else:
        logging.info(f"🆕 STARTING NEW SCRAPING...")
        logging.info(f"   Search: {search_for}")
        logging.info(f"   Target: {total} results")
        logging.info(f"   Output: {output_path}")
    
    try:
        with sync_playwright() as p:
            # Optimized browser setup for performance
            browser_args = [
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--no-sandbox',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-background-timer-throttling',
                '--disable-renderer-backgrounding',
                '--disable-backgrounding-occluded-windows',
                '--disable-background-networking'
            ]
            
            if platform.system() == "Windows":
                browser_path = r"C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
                browser = p.chromium.launch(
                    executable_path=browser_path, 
                    headless=False,
                    args=browser_args
                )
            else:
                browser = p.chromium.launch(headless=False, args=browser_args)
                
            # Create page with optimized settings
            page = browser.new_page()
            
            # Disable images and CSS for faster loading (optional - uncomment if needed)
            # page.route("**/*.{png,jpg,jpeg,gif,svg,css}", lambda route: route.abort())
            
            # Set faster page timeouts
            page.set_default_timeout(5000)  # 5 second default timeout
            page.set_default_navigation_timeout(30000)  # 30 second navigation timeout
            try:
                # Faster page load with reduced timeout
                page.goto("https://www.google.com/maps/@32.9817464,70.1930781,3.67z?", timeout=30000)
                page.wait_for_timeout(1000)  # Reduced from 2000ms
                page.locator('//input[@id="searchboxinput"]').fill(search_for)
                page.keyboard.press("Enter")
                
                # Fast results waiting with shorter timeouts
                try:
                    page.wait_for_selector('//a[contains(@href, "https://www.google.com/maps/place")]', timeout=8000)
                except Exception:
                    # Quick fallback attempt
                    try:
                        page.wait_for_selector('//div[@role="article"]', timeout=5000)
                    except Exception:
                        logging.error(f"No results found for search: {search_for}")
                        return []
                
                # Quick hover check - no need to log warnings
                try:
                    page.hover('//a[contains(@href, "https://www.google.com/maps/place")]')
                except:
                    pass  # Continue without hover
                    
                # Enhanced scrolling to reach the absolute end of results
                previously_counted = 0
                scroll_attempts = 0
                max_scroll_attempts = 200  # Increased limit for thorough searching
                end_of_list_found = False
                
                while scroll_attempts < max_scroll_attempts and not end_of_list_found:
                    # Faster scrolling with bigger increments
                    page.mouse.wheel(0, 15000)  # Increased from 10000
                    page.wait_for_timeout(500)  # Reduced from 1000ms
                    
                    try:
                        found = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').count()
                    except:
                        try:
                            found = page.locator('//div[@role="article"]').count()
                        except:
                            found = 0
                    
                    # Check for "You've reached the end of the list" message using dedicated function
                    try:
                        end_of_list_found = check_end_of_list_reached(page)
                        if end_of_list_found:
                            logging.info(f"🎯 Reached end of list detected by comprehensive checker")
                            break
                    except:
                        pass  # Continue if end message check fails
                    
                    # Less frequent logging for speed
                    if scroll_attempts % 5 == 0:
                        logging.info(f"Currently Found: {found} (scroll attempt {scroll_attempts})")
                        
                    # Break if we have enough results AND found the end
                    if found >= total and end_of_list_found:
                        logging.info(f"✅ Target reached ({found} >= {total}) and end of list confirmed")
                        break
                        
                    # If no new results found for several attempts, try a few more to confirm end
                    if found == previously_counted:
                        # Wait a bit longer and try again to confirm we're really at the end
                        page.wait_for_timeout(1000)
                        
                        # Try aggressive scrolling to ensure we're at the bottom
                        for _ in range(3):
                            page.mouse.wheel(0, 20000)
                            page.wait_for_timeout(300)
                        
                        # Check one more time for new results
                        try:
                            final_found = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').count()
                        except:
                            try:
                                final_found = page.locator('//div[@role="article"]').count()
                            except:
                                final_found = found
                        
                        if final_found == previously_counted:
                            # Still no new results, check for end message one more time using comprehensive checker
                            end_of_list_found = check_end_of_list_reached(page)
                            
                            if end_of_list_found:
                                logging.info(f"🎯 Confirmed end of list detected after no new results")
                            else:
                                logging.info("No new results found and no end message detected - assuming end reached")
                                end_of_list_found = True
                        else:
                            found = final_found
                    
                    previously_counted = found
                    scroll_attempts += 1
                
                if end_of_list_found:
                    logging.info(f"🏁 Successfully scrolled to end of list after {scroll_attempts} attempts")
                else:
                    logging.info(f"⚠️  Reached maximum scroll attempts ({max_scroll_attempts}) without finding end message")
                
                # Fast listings extraction
                try:
                    listings = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').all()[:total]
                    listings = [listing.locator("xpath=..") for listing in listings]
                except:
                    try:
                        listings = page.locator('//div[@role="article"]').all()[:total]
                    except:
                        logging.error("Could not find any listings")
                        return []
                
                logging.info(f"Total Found: {len(listings)}")
                
                # Skip to start_index if resuming
                if start_index > 0:
                    logging.info(f"⏭️  Skipping to index {start_index} (resuming from cache)")
                    listings = listings[start_index:]
                
                scraped_in_session = 0
                total_scraped = existing_count
                
                for idx, listing in enumerate(listings):
                    actual_idx = start_index + idx
                    try:
                        listing.click()
                        
                        # Faster waiting with shorter timeout and simpler selectors
                        try:
                            page.wait_for_selector('//h1[contains(@class, "DUwDvf")]', timeout=5000)
                        except:
                            # Skip if details don't load quickly
                            continue
                        
                        # Reduced wait time for page content loading
                        time.sleep(0.8)  # Reduced from 1.5 seconds
                        
                        place = extract_place(page)
                        if place.name:
                            places.append(place)
                            scraped_in_session += 1
                            total_scraped += 1
                            
                            # Optimized batch saving based on mode with ultra-fast options
                            if scraped_in_session % 10 == 0 or actual_idx == len(listings) - 1:
                                # Use different saving strategies for maximum performance
                                if is_append_mode and ultra_fast_append:
                                    # Ultra-fast: Use batch-optimized for maximum speed
                                    save_places_to_csv_batch_optimized(places, output_path)
                                elif is_append_mode:
                                    # Fast: Use ultra-fast append
                                    save_places_to_csv_ultra_fast(places, output_path)
                                else:
                                    # Regular: Use pandas for first save
                                    save_places_to_csv(places, output_path, append=False)
                                    is_append_mode = True  # Switch to append mode after first save
                                places = []  # Clear list to save memory
                                
                                # Update cache
                                cache_manager.save_cache(
                                    search_for, output_path, total, 
                                    total_scraped, actual_idx
                                )
                                logging.info(f"💾 Progress saved: {total_scraped}/{total} ({actual_idx+1} processed)")
                        else:
                            # Skip logging for performance
                            pass
                    
                    except KeyboardInterrupt:
                        logging.info("🛑 SCRAPING INTERRUPTED BY USER")
                        # Ultra-fast save current progress before exiting
                        if places:
                            if is_append_mode and ultra_fast_append:
                                save_places_to_csv_batch_optimized(places, output_path)
                            elif is_append_mode:
                                save_places_to_csv_ultra_fast(places, output_path)
                            else:
                                save_places_to_csv(places, output_path, append=False)
                        cache_manager.save_cache(
                            search_for, output_path, total, 
                            total_scraped, actual_idx
                        )
                        logging.info(f"💾 Progress saved before exit: {total_scraped}/{total}")
                        logging.info("🔄 Run the same command again to resume from this point!")
                        return places
                    
                    except Exception:
                        # Skip detailed logging for speed - just continue
                        pass
                
                # Clear cache when completed successfully
                if total_scraped >= total:
                    cache_manager.clear_cache(search_for, output_path)
                    logging.info("✅ Scraping completed successfully! Cache cleared.")
                        
            except Exception as page_error:
                logging.error(f"Error during page operations: {page_error}")
                
    except Exception as e:
        logging.error(f"Critical error during scraping: {e}")
        places = []
    return places

def save_places_to_csv(places: List[Place], output_path: str = "result.csv", append: bool = False):
    """Optimized CSV saving with better performance"""
    if not places:
        return
        
    df = pd.DataFrame([asdict(place) for place in places])
    
    # Fast column optimization - only drop columns if they're truly empty
    if not df.empty:
        # Faster column dropping - avoid checking each column individually
        df = df.dropna(axis=1, how='all')  # Drop completely empty columns
        
        file_exists = os.path.isfile(output_path)
        mode = "a" if append else "w"
        header = not (append and file_exists)
        
        # Optimized CSV writing
        df.to_csv(output_path, index=False, mode=mode, header=header, encoding='utf-8')
        logging.info(f"💾 Saved {len(df)} places to {output_path} (append={append})")

def save_places_to_csv_ultra_fast(places: List[Place], output_path: str = "result.csv"):
    """Ultra-fast CSV append mode - bypasses pandas for maximum speed"""
    if not places:
        return
    
    import csv
    
    file_exists = os.path.isfile(output_path)
    
    # Write header only if file doesn't exist
    if not file_exists:
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            if places:
                # Get field names from first place
                fieldnames = list(asdict(places[0]).keys())
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
    
    # Append data directly without pandas processing
    with open(output_path, 'a', newline='', encoding='utf-8') as csvfile:
        if places:
            fieldnames = list(asdict(places[0]).keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write all places at once - much faster than individual writes
            for place in places:
                writer.writerow(asdict(place))
    
    logging.info(f"⚡ Ultra-fast saved {len(places)} places to {output_path}")

def save_places_to_csv_batch_optimized(places: List[Place], output_path: str = "result.csv"):
    """Batch-optimized CSV writing with minimal overhead"""
    if not places:
        return
    
    import csv
    import io
    
    file_exists = os.path.isfile(output_path)
    
    # Prepare data in memory first
    output_buffer = io.StringIO()
    fieldnames = list(asdict(places[0]).keys())
    writer = csv.DictWriter(output_buffer, fieldnames=fieldnames)
    
    # Write header only if file doesn't exist
    if not file_exists:
        writer.writeheader()
    
    # Write all data to buffer at once
    for place in places:
        writer.writerow(asdict(place))
    
    # Write entire buffer to file in one operation
    with open(output_path, 'a' if file_exists else 'w', newline='', encoding='utf-8') as csvfile:
        csvfile.write(output_buffer.getvalue())
    
    output_buffer.close()
    logging.info(f"🚀 Batch-optimized saved {len(places)} places to {output_path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--search", type=str, help="Search query for Google Maps")
    parser.add_argument("-t", "--total", type=int, help="Total number of results to scrape")
    parser.add_argument("-o", "--output", type=str, default="result.csv", help="Output CSV file path")
    parser.add_argument("--append", action="store_true", help="Append results to the output file instead of overwriting")
    parser.add_argument("--ultra-fast", action="store_true", help="Use ultra-fast append mode for maximum speed")
    args = parser.parse_args()
    
    search_for = args.search or "turkish stores in toronto Canada"
    total = args.total or 1
    output_path = args.output
    append = args.append
    ultra_fast = args.ultra_fast
    
    # Auto-enable ultra-fast when append is used
    if append and not ultra_fast:
        ultra_fast = True
        logging.info("⚡ Auto-enabling ultra-fast mode for append operations")
    
    # Check if we're resuming - if cache exists, always append
    cache_manager = CacheManager()
    cache = cache_manager.load_cache(search_for, output_path)
    if cache:
        append = True
        ultra_fast = True  # Auto-enable ultra-fast for resumed operations
        logging.info("🔄 Cache detected - enabling append and ultra-fast mode automatically")
    
    # Use ultra-fast append mode when requested
    places = scrape_places(search_for, total, output_path, ultra_fast_append=ultra_fast)
    
    # Only save remaining places if any (batch saving already handled most)
    if places:
        if ultra_fast:
            save_places_to_csv_batch_optimized(places, output_path)
        else:
            save_places_to_csv(places, output_path, append=append)

if __name__ == "__main__":
    main()
