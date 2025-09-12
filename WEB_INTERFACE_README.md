# Web Interface for Google Maps Scraper

This is a Flask-based web interface for the Google Maps Scraper that provides a user-friendly way to interact with the scraping functionality without using command-line.

## Features

- **Easy-to-use Web Interface**: No need to use command line
- **Real-time Progress Monitoring**: Watch your scraping jobs in real-time
- **Job History**: View all previous scraping jobs and their results
- **Results Preview**: Preview scraped data before downloading
- **Download Results**: Download CSV files directly from the web interface
- **Background Processing**: Jobs run in the background, so you can close the browser

## How to Run

1. **Start the Web Server**:
   ```bash
   C:/Python313/python.exe app.py
   ```

2. **Open Your Browser**:
   - Navigate to `http://localhost:5000`
   - Or `http://127.0.0.1:5000`

3. **Use the Interface**:
   - Enter your search query (e.g., "Turkish restaurants in Toronto Canada")
   - Set the number of results you want to scrape
   - Choose your output file name
   - Optionally enable append mode
   - Click "Start Scraping"

## Web Interface Pages

### Home Page (`/`)
- Main scraping form
- Feature overview
- Start new scraping jobs

### Monitor Page (`/monitor/<job_id>`)
- Real-time progress tracking
- Job status updates
- Download and preview options
- Error messages if any

### History Page (`/history`)
- List of all previous jobs
- Job status and results
- Quick access to download results

## API Endpoints

- `POST /start_scraping` - Start a new scraping job
- `GET /monitor/<job_id>` - Monitor a specific job
- `GET /job_status/<job_id>` - Get job status (JSON)
- `GET /download/<job_id>` - Download results file
- `GET /preview/<job_id>` - Preview results (JSON)
- `GET /history` - View job history

## Technical Details

- **Framework**: Flask 3.0.0
- **Frontend**: Bootstrap 5, Font Awesome icons
- **Background Processing**: Threading
- **Real-time Updates**: JavaScript polling
- **File Management**: CSV download and preview

## Security Notes

- The web server runs on `0.0.0.0:5000` by default
- Change the `secret_key` in `app.py` for production use
- Consider adding authentication for production deployment

## Original Command Line Interface

The original command-line interface still works exactly as before:

```bash
C:/Python313/python.exe main.py -s "search query" -t 10 -o output.csv
```

The web interface is just an additional layer that calls the same underlying scraping functionality.
