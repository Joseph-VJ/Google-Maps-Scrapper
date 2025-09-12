# Chennai Area-Specific Scraper

This feature allows you to scrape business data from specific areas in Chennai, Tamil Nadu, India. Instead of doing a general search, you can target exact neighborhoods and localities for more precise data collection.

## Features

### 🗺️ Comprehensive Area Coverage
- **75+ Chennai Areas** including all major localities
- **Organized by Zones**: Central, South, North, West Chennai
- **IT Corridor**: Special focus on tech hubs like OMR, Thoraipakkam
- **City Center**: Historic areas like George Town, Mount Road
- **Outer Areas**: Extended Chennai including Chengalpattu, Kancheepuram

### 🏢 Business Type Selection
Choose from 25+ business categories:
- Restaurants, Hotels, Hospitals
- Banks, ATMs, Petrol Pumps
- Shopping Malls, Supermarkets
- Schools, Gyms, Beauty Salons
- Electronics, Textiles, Jewelry
- Professional Services (Doctors, Lawyers, CAs)
- And many more...

### 🎯 Smart Area Selection
- **Select All/Deselect All** buttons for quick selection
- **Visual indicators** showing area zones with color coding
- **Counter** showing how many areas selected
- **Validation** ensures at least one area is selected

### 📊 Advanced Monitoring
- **Real-time progress** for each area being scraped
- **Area-by-area status** with visual indicators:
  - ✅ Completed areas (green check)
  - 🔄 Currently scraping (spinning icon)
  - ⏰ Waiting areas (clock icon)
- **Results counter** for each area
- **Error handling** with specific area error messages

## How to Use

### 1. Access Chennai Scraper
- Go to the main page and click "Chennai Area Scraper"
- Or navigate directly to `/chennai` in your browser

### 2. Configure Your Search
- **Business Type**: Select what type of businesses you want to find
- **Results Per Area**: How many results to get from each area (1-100)
- **Output File**: Name your CSV file (default: chennai-results.csv)
- **Append Mode**: Add to existing file instead of overwriting

### 3. Select Areas
- Browse the organized list of Chennai areas
- Use "Select All" to choose everything or pick specific areas
- Areas are grouped by:
  - **Central Chennai**: T. Nagar, Anna Nagar, Vadapalani, etc.
  - **South Chennai**: Adyar, Velachery, Tambaram, etc.
  - **North Chennai**: Ambattur, Avadi, Red Hills, etc.
  - **West Chennai**: Porur, Chromepet, Pallavaram, etc.
  - **IT Corridor**: Thoraipakkam, Navalur, Siruseri, etc.

### 4. Start Scraping
- Click "Start Chennai Area Scraping"
- Monitor progress in real-time
- Download results when complete

## Area Coverage

### Central Chennai (10 areas)
T. Nagar, Anna Nagar, Vadapalani, Kodambakkam, Nungambakkam, Egmore, Chetpet, Kilpauk, Purasawalkam, Thousand Lights

### South Chennai (12 areas)
Adyar, Velachery, Tambaram, Guindy, Pallikaranai, Mylapore, Besant Nagar, Thiruvanmiyur, Sholinganallur, OMR, Perungudi, Taramani

### North Chennai (10 areas)
Ambattur, Avadi, Red Hills, Madhavaram, Perambur, Kolathur, Villivakkam, Thirumullaivoyal, Poonamallee, Manali

### West Chennai (10 areas)
Porur, Chromepet, Pallavaram, Ashok Nagar, KK Nagar, West Mambalam, Saidapet, Meenambakkam, Mangadu, Kundrathur

### IT Corridor (5 areas)
Thoraipakkam, Navalur, Siruseri, Kelambakkam, Medavakkam

### Outer Areas (5 areas)
Chengalpattu, Kancheepuram, Maraimalai Nagar, Vandalur, Urapakkam

### City Center (5 areas)
George Town, Parrys Corner, Mount Road, Triplicane, Fort St. George

### Additional Areas (5 areas)
ECR, GST Road, Rajiv Gandhi Salai, Mahindra City, Mahabalipuram

## Example Searches

### Restaurants in South Chennai
- Business Type: Restaurants
- Selected Areas: Adyar, Velachery, Mylapore, Besant Nagar
- Results Per Area: 20
- Expected Total: 80 restaurants

### Banks in IT Corridor
- Business Type: Banks
- Selected Areas: Thoraipakkam, Navalur, Siruseri, Sholinganallur
- Results Per Area: 10
- Expected Total: 40 bank branches

### Hospitals Across All Chennai
- Business Type: Hospitals
- Selected Areas: All (75+ areas)
- Results Per Area: 5
- Expected Total: 375+ hospitals

## Output Format

Results are saved in CSV format with columns:
- Name, Address, Website, Phone Number
- Reviews Count, Reviews Average
- Store Shopping, In-store Pickup, Store Delivery
- Place Type, Opens At, Introduction

## Tips for Best Results

1. **Start Small**: Try 5-10 areas first to test
2. **Reasonable Limits**: Use 10-20 results per area for good coverage
3. **Business Matching**: Some areas may have more/fewer businesses of certain types
4. **File Management**: Use descriptive filenames like "chennai-restaurants-south.csv"
5. **Append Mode**: Use when adding to existing data collections

## Technical Details

- Each area is searched individually with the format: "{business_type} in {area}, Chennai, Tamil Nadu, India"
- Results are automatically compiled into a single CSV file
- Progress tracking shows completion status for each area
- Background processing allows browser closing during scraping
- Error handling reports issues with specific areas

This Chennai area feature makes the Google Maps Scraper much more precise and useful for local business research in Chennai!
