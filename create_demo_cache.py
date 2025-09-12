#!/usr/bin/env python3
"""
Create a demo cache scenario for testing the web interface.
"""

from main import CacheManager
import pandas as pd
import os

def create_demo_cache():
    """Create a demo cache that can be detected by the web interface."""
    
    # Parameters for a realistic scenario
    search_query = "restaurants in Chennai"
    output_file = "demo_restaurants.csv"
    
    print(f"Creating demo cache for: '{search_query}' -> '{output_file}'")
    
    # Create a partial CSV file to simulate interrupted scraping
    demo_data = []
    for i in range(15):
        demo_data.append({
            "name": f"Restaurant {i+1}",
            "address": f"Street {i+1}, Chennai",
            "website": f"https://restaurant{i+1}.com",
            "phone_number": f"+91-{9000000000 + i}",
            "reviews_count": 50 + i*10,
            "reviews_average": 4.0 + (i % 5) * 0.2,
            "store_shopping": "No",
            "in_store_pickup": "Yes",
            "store_delivery": "Yes",
            "place_type": "Restaurant",
            "opens_at": "9:00 AM",
            "introduction": f"Great restaurant number {i+1} in Chennai"
        })
    
    df = pd.DataFrame(demo_data)
    df.to_csv(output_file, index=False)
    print(f"✓ Created {output_file} with {len(demo_data)} records")
    
    # Create cache showing we were interrupted at 15 out of 100 results
    cache_manager = CacheManager()
    cache_id = cache_manager.save_cache(
        search_query=search_query,
        output_file=output_file,
        total_target=100,
        scraped_count=15,
        last_scraped_index=15
    )
    
    print(f"✓ Cache created with ID: {cache_id}")
    print(f"✓ Progress: 15/100 items scraped")
    print("\nNow you can:")
    print("1. Open the web interface at http://127.0.0.1:5000")
    print("2. Enter the same search query: 'restaurants in Chennai'")
    print("3. Use the same output filename: 'demo_restaurants.csv'")
    print("4. The interface should detect the cache and show resume options!")
    
    return search_query, output_file

if __name__ == "__main__":
    create_demo_cache()
