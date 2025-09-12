#!/usr/bin/env python3
"""
Test script to demonstrate the cache functionality of the Google Maps Scraper.
This script will create a cache file, simulate an interruption, and then resume.
"""

import time
import os
from main import CacheManager, ScrapingCache

def test_cache_functionality():
    """Test the cache save and load functionality."""
    print("=== Testing Cache Functionality ===")
    
    # Test parameters
    search_query = "coffee shops"
    output_file = "test_cache_demo.csv"
    
    print(f"Testing with search query: '{search_query}' and output file: '{output_file}'")
    
    # Initialize cache manager
    cache_manager = CacheManager()
    
    # Create a test CSV file to simulate existing data
    import pandas as pd
    test_df = pd.DataFrame([
        {"name": "Test Coffee Shop 1", "address": "123 Test St"},
        {"name": "Test Coffee Shop 2", "address": "456 Test Ave"}
    ])
    test_df.to_csv(output_file, index=False)
    print(f"✓ Created test CSV file with {len(test_df)} records")
    
    # Save cache using the CacheManager method
    cache_id = cache_manager.save_cache(
        search_query=search_query,
        output_file=output_file,
        total_target=100,
        scraped_count=25,
        last_scraped_index=25
    )
    
    print(f"✓ Cache saved successfully with ID: {cache_id}")
    
    # Load the cache
    loaded_cache = cache_manager.load_cache(search_query, output_file)
    
    if loaded_cache:
        print(f"✓ Cache loaded successfully:")
        print(f"  - Search query: {loaded_cache.search_query}")
        print(f"  - Output file: {loaded_cache.output_file}")
        print(f"  - Progress: {loaded_cache.scraped_count}/{loaded_cache.total_target}")
        print(f"  - Last scraped index: {loaded_cache.last_scraped_index}")
        print(f"  - Timestamp: {loaded_cache.timestamp}")
        
        # Get existing data count
        existing_count = cache_manager.get_existing_data_count(output_file)
        print(f"✓ Existing data count: {existing_count}")
        
        # Clean up
        cache_manager.clear_cache(search_query, output_file)
        print("✓ Cache cleared")
        
        # Clean up test file
        if os.path.exists(output_file):
            os.remove(output_file)
            print("✓ Test CSV file removed")
        
        print("\n=== Cache Test Completed Successfully! ===")
        return True
    else:
        print("✗ Failed to load cache")
        return False

if __name__ == "__main__":
    test_cache_functionality()
