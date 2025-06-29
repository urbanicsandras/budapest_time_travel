import requests
from bs4 import BeautifulSoup
import re
import os
import zipfile
from urllib.parse import urljoin, urlparse
from datetime import datetime
import time
from collections import defaultdict

class TransitFeedDownloader:
    def __init__(self, base_url="https://transitfeeds.com/p/bkk/42"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
    def get_all_pages(self, start_page=1, direction="forward"):
        """Get all available pages from the transit feed site"""
        feeds = []
        seen_feeds = set()  # Track unique feeds to detect duplicates
        
        if direction == "forward":
            print(f"Fetching pages forward from {start_page}...")
            current_page = start_page
            duplicate_pages = 0  # Count consecutive pages with only duplicate content
            
            while True:
                print(f"Fetching page {current_page}...")
                url = f"{self.base_url}?p={current_page}"
                
                try:
                    response = self.session.get(url)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    page_feeds = self.parse_feeds_from_page(soup)
                    
                    if not page_feeds:
                        print(f"No feeds found on page {current_page}. Stopping.")
                        break
                    
                    # Check for new feeds (not duplicates)
                    new_feeds = []
                    for feed in page_feeds:
                        # Create a unique identifier for each feed
                        feed_id = f"{feed['original_date']}_{feed['version']}_{feed['size']}"
                        if feed_id not in seen_feeds:
                            seen_feeds.add(feed_id)
                            new_feeds.append(feed)
                    
                    if new_feeds:
                        feeds.extend(new_feeds)
                        print(f"Found {len(new_feeds)} new feeds on page {current_page} ({len(page_feeds)} total)")
                        duplicate_pages = 0  # Reset duplicate counter
                    else:
                        duplicate_pages += 1
                        print(f"Page {current_page} contains only duplicate feeds ({duplicate_pages} consecutive duplicate pages)")
                        
                        # If we've seen 3 consecutive pages with only duplicates, we've likely reached the end
                        if duplicate_pages >= 3:
                            print(f"Detected repeated content for {duplicate_pages} consecutive pages. Reached end of available pages.")
                            break
                    
                    current_page += 1
                    time.sleep(1)  # Be respectful to the server
                    
                except requests.HTTPError as e:
                    if response.status_code == 404:
                        print(f"Page {current_page} not found (404). Reached end of available pages.")
                        break
                    else:
                        print(f"HTTP error on page {current_page}: {e}")
                        break
                except requests.RequestException as e:
                    print(f"Error fetching page {current_page}: {e}")
                    break
                    
        else:  # backward
            print(f"Fetching pages backward from {start_page} to 1...")
            current_page = start_page
            
            while current_page >= 1:
                print(f"Fetching page {current_page}...")
                url = f"{self.base_url}?p={current_page}"
                
                try:
                    response = self.session.get(url)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    page_feeds = self.parse_feeds_from_page(soup)
                    
                    if not page_feeds:
                        print(f"No feeds found on page {current_page}.")
                    else:
                        # For backward direction, we don't need to check for duplicates as strictly
                        # since we know the page range
                        feeds.extend(page_feeds)
                        print(f"Found {len(page_feeds)} feeds on page {current_page}")
                    
                    current_page -= 1
                    time.sleep(1)  # Be respectful to the server
                    
                except requests.HTTPError as e:
                    if response.status_code == 404:
                        print(f"Page {current_page} not found (404). Skipping.")
                        current_page -= 1
                        continue
                    else:
                        print(f"HTTP error on page {current_page}: {e}")
                        current_page -= 1
                        continue
                except requests.RequestException as e:
                    print(f"Error fetching page {current_page}: {e}")
                    current_page -= 1
                    continue
            
            print(f"Finished fetching all pages from {start_page} down to 1")
        
        return feeds
    
    def parse_feeds_from_page(self, soup):
        """Parse feed information from a single page"""
        feeds = []
        
        # Look for the main table containing feed data
        table = soup.find('table') or soup.find('div', class_='table')
        if not table:
            return feeds
            
        # Find all table rows, skip header row
        rows = table.find_all('tr')
        
        if len(rows) <= 1:
            return feeds
            
        # Skip header row
        data_rows = rows[1:]
        
        for row in data_rows:
            try:
                feed_info = self.extract_feed_info(row)
                if feed_info:
                    feeds.append(feed_info)
            except Exception as e:
                continue
                
        return feeds
    
    def extract_feed_info(self, row):
        """Extract feed information from table row"""
        cells = row.find_all(['td', 'th'])
        
        if len(cells) < 4:  # Need at least Date, Size, Routes, Status columns
            return None
            
        try:
            # Check if this is the 5-column format (with Version) or 4-column format (without Version)
            has_version_column = len(cells) >= 5
            
            if has_version_column:
                # 5-column format: Date, Version, Size, Routes, Status
                date_cell = cells[0]
                version_cell = cells[1]
                size_cell = cells[2]
                status_cell = cells[-1]
                
                date_text = date_cell.get_text().strip()
                version = version_cell.get_text().strip()
                size = size_cell.get_text().strip()
                
            else:
                # 4-column format: Date, Size, Routes, Status (no Version column)
                date_cell = cells[0]
                size_cell = cells[1]
                status_cell = cells[-1]
                
                date_text = date_cell.get_text().strip()
                version = None  # No version column
                size = size_cell.get_text().strip()
            
            # Parse the date from the date column
            date_str = self.parse_date(date_text)
            
            # Try to extract date from version if available
            version_date = None
            if version:
                version_date = self.extract_date_from_version(version)
            
            # Look for download link in the last column (Status column)
            download_link = status_cell.find('a', string=re.compile(r'Download', re.I))
            
            if not download_link:
                # Also check other cells for download link
                for cell in cells:
                    download_link = cell.find('a', string=re.compile(r'Download', re.I))
                    if download_link:
                        break
            
            if not download_link:
                return None
                
            download_url = urljoin(self.base_url, download_link.get('href'))
            
            # Create version fallback for files without version column
            if not version:
                version = f"NO_VERSION_{date_str}" if date_str else "NO_VERSION_NO_DATE"
            
            return {
                'download_url': download_url,
                'version': version,
                'date': date_str,
                'size': size,
                'raw_text': f"{date_text}" + (f" - {version}" if version and not version.startswith("NO_VERSION") else ""),
                'original_date': date_text,
                'has_version': has_version_column,
                'version_date': version_date  # Date extracted from version if any
            }
            
        except Exception as e:
            return None
    
    def has_next_page(self, soup):
        """Check if there's a next page available"""
        # Look for pagination links
        next_link = soup.find('a', string=re.compile(r'next', re.I)) or \
                   soup.find('a', href=re.compile(r'p=\d+')) and soup.find('a', string=re.compile(r'>', re.I)) or \
                   soup.find('a', class_=lambda x: x and 'next' in x.lower()) or \
                   soup.find('a', attrs={'aria-label': re.compile(r'next', re.I)})
        
        if next_link:
            print(f"DEBUG: Found next page link: {next_link}")
            return True
        
        # Alternative: look for numbered pagination
        pagination = soup.find_all('a', href=re.compile(r'p=\d+'))
        if pagination:
            print(f"DEBUG: Found {len(pagination)} pagination links")
            return len(pagination) > 1  # If there are multiple page links, assume there might be more
            
        return False

    def parse_date(self, date_text):
        """Parse date from various formats to YYYYMMDD"""
        date_text = date_text.strip()
        
        # Handle "19 June 2015" format
        try:
            date_obj = datetime.strptime(date_text, '%d %B %Y')
            return date_obj.strftime('%Y%m%d')
        except ValueError:
            pass
        
        # Handle other common formats
        date_formats = [
            '%Y-%m-%d',      # 2015-06-19
            '%m/%d/%Y',      # 06/19/2015
            '%d/%m/%Y',      # 19/06/2015
            '%Y%m%d',        # 20150619
            '%B %d, %Y',     # June 19, 2015
            '%d %b %Y',      # 19 Jun 2015
        ]
        
        for fmt in date_formats:
            try:
                date_obj = datetime.strptime(date_text, fmt)
                return date_obj.strftime('%Y%m%d')
            except ValueError:
                continue
        
        # If all else fails, try to extract date from version string
        # Look for YYYYMMDD pattern in the text
        date_match = re.search(r'(\d{8})', date_text)
        if date_match:
            potential_date = date_match.group(1)
            try:
                # Validate it's a real date
                datetime.strptime(potential_date, '%Y%m%d')
                return potential_date
            except ValueError:
                pass
        
        print(f"Could not parse date: {date_text}")
        return None
        """Check if there's a next page available"""
        next_link = soup.find('a', string=re.compile(r'next', re.I)) or \
                   soup.find('a', href=re.compile(r'p=\d+')) or \
                   soup.find('a', class_=lambda x: x and 'next' in x.lower())
        return next_link is not None
    
    def extract_date_from_version(self, version):
        """Extract date from version string (e.g., 1153.20231226 → 20231226)"""
        if not version:
            return None
        
        # Look for YYYYMMDD pattern in version (handles both L462-20150620 and 1153.20231226 formats)
        date_match = re.search(r'(\d{8})', version)
        if date_match:
            potential_date = date_match.group(1)
            try:
                # Validate it's a real date
                datetime.strptime(potential_date, '%Y%m%d')
                return potential_date
            except ValueError:
                pass
        
        return None

    def generate_filename(self, feed):
        """Generate filename based on date - always YYYYMMDD.zip format"""
        # Priority 1: Use date from version if available
        if feed.get('version_date'):
            return f"{feed['version_date']}.zip"
        # Priority 2: Use parsed date from date column
        elif feed['date']:
            return f"{feed['date']}.zip"
        else:
            print(f"    WARNING: Could not parse date for '{feed['original_date']}', skipping this feed")
            return None

    def resolve_filename_conflicts(self, feeds):
        """Resolve filename conflicts based on version and date priority"""
        filename_groups = defaultdict(list)
        valid_feeds = []
        
        # First, filter out feeds where we couldn't generate a filename
        for feed in feeds:
            filename = self.generate_filename(feed)
            if filename:  # Only process feeds with valid filenames
                filename_groups[filename].append(feed)
                valid_feeds.append(feed)
        
        resolved_feeds = []
        
        for filename, feed_group in filename_groups.items():
            if len(feed_group) == 1:
                resolved_feeds.append(feed_group[0])
            else:
                # Handle conflicts as specified in requirements
                print(f"\nConflict detected for filename {filename}:")
                for feed in feed_group:
                    version_info = f" (Version: {feed['version']})" if feed.get('has_version') and feed['version'] and not feed['version'].startswith("NO_VERSION") else " (no version)"
                    print(f"  - {feed['original_date']}{version_info}")
                
                best_feed = self.select_best_feed(feed_group)
                resolved_feeds.append(best_feed)
                version_info = f" (Version: {best_feed['version']})" if best_feed.get('has_version') and best_feed['version'] and not best_feed['version'].startswith("NO_VERSION") else " (no version)"
                print(f"  → Selected: {best_feed['original_date']}{version_info}")
        
        return resolved_feeds
    
    def select_best_feed(self, feed_group):
        """Select the best feed from a group of conflicting feeds"""
        # Requirements: If same dates, compare versions. 
        # But first check if version contains date info.
        
        def sort_key(feed):
            # Primary sort: Use version date if available, otherwise use parsed date
            primary_date = feed.get('version_date') or feed['date'] or '00000000'
            
            # Secondary sort: Version number for tie-breaking
            version_score = self.extract_version_number(feed['version'])
            
            return (primary_date, version_score)
        
        # Sort by date first (descending), then by version (descending)
        sorted_feeds = sorted(feed_group, key=sort_key, reverse=True)
        return sorted_feeds[0]
    
    def extract_version_number(self, version):
        """Extract numeric part from version for comparison"""
        if not version:
            return 0
        
        # For versions like "L462-20150620", extract the number after L
        match = re.search(r'L(\d+)', version)
        if match:
            return int(match.group(1))
        
        # For other version formats, extract all numbers
        numbers = re.findall(r'\d+', version)
        if numbers:
            return int(numbers[0])  # Use first number found
        
        return 0
    
    def download_feeds(self, feeds, output_dir="transit_feeds"):
        """Download all feeds to the specified directory"""
        os.makedirs(output_dir, exist_ok=True)
        
        successful_downloads = []
        failed_downloads = []
        skipped_downloads = []
        
        for i, feed in enumerate(feeds, 1):
            filename = self.generate_filename(feed)
            filepath = os.path.join(output_dir, filename)
            
            # Check if file already exists
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                if file_size > 0:  # Make sure it's not an empty file
                    print(f"⏭ Skipping {i}/{len(feeds)}: {filename} (already exists, {file_size:,} bytes)")
                    skipped_downloads.append(filename)
                    continue
                else:
                    print(f"⚠ Found empty file {filename}, will re-download")
                    os.remove(filepath)
            
            print(f"⬇ Downloading {i}/{len(feeds)}: {filename}")
            
            try:
                response = self.session.get(feed['download_url'], stream=True)
                response.raise_for_status()
                
                # Get expected file size from headers if available
                expected_size = response.headers.get('content-length')
                if expected_size:
                    expected_size = int(expected_size)
                    print(f"  Expected size: {expected_size:,} bytes")
                
                # Download with progress indication for large files
                downloaded_size = 0
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Show progress for files > 10MB
                        if expected_size and expected_size > 10*1024*1024:
                            progress = (downloaded_size / expected_size) * 100
                            print(f"\r  Progress: {progress:.1f}%", end='', flush=True)
                
                if expected_size and expected_size > 10*1024*1024:
                    print()  # New line after progress
                
                # Verify download completed successfully
                final_size = os.path.getsize(filepath)
                if final_size == 0:
                    os.remove(filepath)
                    raise Exception("Downloaded file is empty")
                
                print(f"✓ Downloaded: {filename} ({final_size:,} bytes)")
                successful_downloads.append(filename)
                
            except Exception as e:
                print(f"✗ Failed to download {filename}: {e}")
                failed_downloads.append((filename, str(e)))
                
                # Clean up partial download
                if os.path.exists(filepath):
                    os.remove(filepath)
            
            time.sleep(1)  # Be respectful to the server
        
        return successful_downloads, failed_downloads, skipped_downloads
    
    def create_master_zip(self, output_dir="transit_feeds", master_zip="all_transit_feeds.zip"):
        """Create a master zip file containing all downloaded feeds"""
        if not os.path.exists(output_dir):
            print(f"Output directory {output_dir} doesn't exist")
            return
        
        zip_files = [f for f in os.listdir(output_dir) if f.endswith('.zip')]
        
        if not zip_files:
            print("No zip files found to combine")
            return
        
        # Don't include the master zip in itself
        zip_files = [f for f in zip_files if f != master_zip]
        
        if not zip_files:
            print("No zip files found to combine (excluding master zip)")
            return
            
        print(f"Creating master zip with {len(zip_files)} files...")
        
        with zipfile.ZipFile(master_zip, 'w', zipfile.ZIP_DEFLATED) as master:
            for zip_file in sorted(zip_files):  # Sort for consistent ordering
                zip_path = os.path.join(output_dir, zip_file)
                file_size = os.path.getsize(zip_path)
                master.write(zip_path, zip_file)
                print(f"  Added {zip_file} ({file_size:,} bytes)")
        
        master_size = os.path.getsize(master_zip)
        print(f"Master zip created: {master_zip} ({master_size:,} bytes)")

def main():
    import argparse
    
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='Download BKK transit feeds')
    parser.add_argument('--output', '-o', default='transit_feeds', 
                       help='Output directory for downloaded files (default: transit_feeds)')
    parser.add_argument('--page', '-p', type=int, default=1,
                       help='Starting page number (default: 1)')
    parser.add_argument('--direction', '-d', choices=['forward', 'backward'], default='forward',
                       help='Direction to fetch pages: forward (1→2→3...) or backward (232→231→230...) (default: forward)')
    
    args = parser.parse_args()
    
    downloader = TransitFeedDownloader()
    
    print("Starting transit feed download process...")
    print(f"Output directory: {args.output}")
    print(f"Starting from page: {args.page}")
    print(f"Direction: {args.direction}")
    
    # Get all feeds from all pages starting from specified page in specified direction
    print("Fetching feed information from all pages...")
    all_feeds = downloader.get_all_pages(args.page, args.direction)
    
    if not all_feeds:
        print("No feeds found!")
        return
    
    print(f"Found {len(all_feeds)} total feeds")
    
    # Resolve filename conflicts
    print("Resolving filename conflicts...")
    resolved_feeds = downloader.resolve_filename_conflicts(all_feeds)
    
    print(f"After conflict resolution: {len(resolved_feeds)} feeds to download")
    
    # Download all feeds to specified directory
    print("Starting downloads...")
    successful, failed, skipped = downloader.download_feeds(resolved_feeds, args.output)
    
    print(f"\nDownload Summary:")
    print(f"Downloaded to: {args.output}")
    print(f"Successful: {len(successful)}")
    print(f"Skipped (already exist): {len(skipped)}")
    print(f"Failed: {len(failed)}")
    
    if skipped:
        print(f"\nSkipped files (already downloaded):")
        for filename in skipped:
            print(f"  {filename}")
    
    if failed:
        print("\nFailed downloads:")
        for filename, error in failed:
            print(f"  {filename}: {error}")
    
    print("Process completed!")

if __name__ == "__main__":
    main()

### RUN COMMAND
# in 'budapest_time_travel\src\data_downloader' folder
# python transit_downloader.py -o ../../data/raw -p 1 -d backward