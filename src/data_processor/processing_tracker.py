"""
Processing history tracker for transit data processing.
Keeps track of which dates have been successfully processed.
"""
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Set
from .config import Config


class ProcessingTracker:
    """Tracks processing history and determines which dates need processing."""
    
    def __init__(self, data_folder: Optional[str] = None):
        """
        Initialize the processing tracker.
        
        Args:
            data_folder: Path to processed data folder. If None, uses auto-detected path.
        """
        if data_folder is None:
            data_folder = Config.get_default_processed_data_folder()
        
        self.data_folder = data_folder
        self.tracker_file = os.path.join(data_folder, 'processing_history.json')
        self.history = self._load_history()
    
    def _load_history(self) -> Dict:
        """Load processing history from file."""
        if os.path.exists(self.tracker_file):
            try:
                with open(self.tracker_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                print("Warning: Could not read processing history file. Starting fresh.")
                return self._create_empty_history()
        else:
            return self._create_empty_history()
    
    def _create_empty_history(self) -> Dict:
        """Create empty processing history structure."""
        return {
            'last_update': None,
            'last_successful_date': None,
            'processed_dates': [],
            'failed_dates': [],
            'processing_sessions': []
        }
    
    def _save_history(self) -> None:
        """Save processing history to file."""
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.tracker_file), exist_ok=True)
        
        try:
            with open(self.tracker_file, 'w') as f:
                json.dump(self.history, f, indent=2, default=str)
        except IOError as e:
            print(f"Warning: Could not save processing history: {e}")
    
    def get_available_dates(self, raw_data_folder: Optional[str] = None) -> Set[str]:
        """
        Get all available dates from raw data folder.
        
        Args:
            raw_data_folder: Path to raw data folder. If None, uses auto-detected path.
            
        Returns:
            Set of available date strings
        """
        if raw_data_folder is None:
            raw_data_folder = Config.get_default_raw_data_folder()
        
        available_dates = set()
        
        if os.path.exists(raw_data_folder):
            for item in os.listdir(raw_data_folder):
                item_path = os.path.join(raw_data_folder, item)
                if os.path.isdir(item_path) or item.endswith('.zip'):
                    date_name = item.replace('.zip', '')
                    # Validate date format (YYYYMMDD)
                    if len(date_name) == 8 and date_name.isdigit():
                        try:
                            # Verify it's a valid date
                            datetime.strptime(date_name, '%Y%m%d')
                            available_dates.add(date_name)
                        except ValueError:
                            pass  # Skip invalid dates
        
        return available_dates
    
    def get_last_processed_date(self, available_dates: Optional[Set[str]] = None) -> Optional[str]:
        """
        Get the last successfully processed date that still has available data.
        
        Args:
            available_dates: Set of available dates. If None, will be determined automatically.
            
        Returns:
            Last processed date string or None if no dates processed
        """
        if available_dates is None:
            available_dates = self.get_available_dates()
        
        # Get processed dates that still have available data
        processed_and_available = [
            date for date in self.history['processed_dates'] 
            if date in available_dates
        ]
        
        if processed_and_available:
            # Return the latest processed date
            return max(processed_and_available)
        
        return None
    
    def get_dates_to_process(self, start_date: str, end_date: str, 
                           raw_data_folder: Optional[str] = None) -> List[str]:
        """
        Get list of dates that need to be processed.
        
        Args:
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            raw_data_folder: Path to raw data folder
            
        Returns:
            List of dates that need processing
        """
        available_dates = self.get_available_dates(raw_data_folder)
        last_processed = self.get_last_processed_date(available_dates)
        
        # Generate all dates in the requested range
        start = datetime.strptime(start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')
        
        all_requested_dates = []
        current = start
        while current <= end:
            all_requested_dates.append(current.strftime('%Y%m%d'))
            current += timedelta(days=1)
        
        # Filter to only available dates
        available_requested_dates = [date for date in all_requested_dates if date in available_dates]
        
        if last_processed:
            # Only process dates after the last processed date
            dates_to_process = [date for date in available_requested_dates if date > last_processed]
            
            if dates_to_process:
                print(f"📅 Last processed date: {last_processed}")
                print(f"🔄 Resuming from: {dates_to_process[0]}")
                print(f"📊 Processing {len(dates_to_process)} new dates out of {len(available_requested_dates)} available")
            else:
                print(f"✅ All dates up to {end_date} have already been processed!")
                print(f"📅 Last processed: {last_processed}")
        else:
            dates_to_process = available_requested_dates
            print(f"🆕 Starting fresh processing")
            print(f"📊 Processing {len(dates_to_process)} dates")
        
        return dates_to_process
    
    def record_processing_session(self, start_date: str, end_date: str, 
                                successful_dates: List[str], failed_dates: List[str]) -> None:
        """
        Record the results of a processing session.
        
        Args:
            start_date: Start date of the session
            end_date: End date of the session
            successful_dates: List of successfully processed dates
            failed_dates: List of failed dates
        """
        session = {
            'timestamp': datetime.now().isoformat(),
            'start_date': start_date,
            'end_date': end_date,
            'successful_count': len(successful_dates),
            'failed_count': len(failed_dates),
            'successful_dates': successful_dates,
            'failed_dates': failed_dates
        }
        
        # Add to history
        self.history['processing_sessions'].append(session)
        
        # Update processed dates (add new successful dates)
        existing_processed = set(self.history['processed_dates'])
        existing_processed.update(successful_dates)
        self.history['processed_dates'] = sorted(list(existing_processed))
        
        # Update failed dates (but don't remove them if they succeed later)
        existing_failed = set(self.history['failed_dates'])
        # Remove dates that succeeded this time
        existing_failed -= set(successful_dates)
        # Add new failed dates
        existing_failed.update(failed_dates)
        self.history['failed_dates'] = sorted(list(existing_failed))
        
        # Update last successful date and last update time
        if successful_dates:
            latest_successful = max(successful_dates)
            if (self.history['last_successful_date'] is None or 
                latest_successful > self.history['last_successful_date']):
                self.history['last_successful_date'] = latest_successful
        
        self.history['last_update'] = datetime.now().isoformat()
        
        # Save to file
        self._save_history()
    
    def get_processing_summary(self) -> Dict:
        """Get a summary of processing history."""
        available_dates = self.get_available_dates()
        
        summary = {
            'total_available_dates': len(available_dates),
            'total_processed_dates': len(self.history['processed_dates']),
            'total_failed_dates': len(self.history['failed_dates']),
            'last_successful_date': self.history['last_successful_date'],
            'last_update': self.history['last_update'],
            'processing_sessions': len(self.history['processing_sessions']),
            'success_rate': 0.0
        }
        
        if summary['total_processed_dates'] + summary['total_failed_dates'] > 0:
            summary['success_rate'] = (
                summary['total_processed_dates'] / 
                (summary['total_processed_dates'] + summary['total_failed_dates'])
            ) * 100
        
        return summary
    
    def print_summary(self) -> None:
        """Print a nice summary of processing history."""
        summary = self.get_processing_summary()
        
        print("📊 Processing History Summary")
        print("=" * 40)
        print(f"Available dates: {summary['total_available_dates']}")
        print(f"Successfully processed: {summary['total_processed_dates']}")
        print(f"Failed dates: {summary['total_failed_dates']}")
        print(f"Success rate: {summary['success_rate']:.1f}%")
        print(f"Last successful date: {summary['last_successful_date'] or 'None'}")
        print(f"Last update: {summary['last_update'] or 'Never'}")
        print(f"Processing sessions: {summary['processing_sessions']}")
        
        if self.history['failed_dates']:
            print(f"\n❌ Failed dates ({len(self.history['failed_dates'])}):")
            # Show first few failed dates
            failed_sample = self.history['failed_dates'][:10]
            print(f"   {', '.join(failed_sample)}")
            if len(self.history['failed_dates']) > 10:
                print(f"   ... and {len(self.history['failed_dates']) - 10} more")
    
    def reset_history(self) -> None:
        """Reset processing history (use with caution!)."""
        self.history = self._create_empty_history()
        self._save_history()
        print("⚠️  Processing history has been reset!")
    
    def mark_date_as_processed(self, date: str) -> None:
        """Manually mark a date as processed."""
        if date not in self.history['processed_dates']:
            self.history['processed_dates'].append(date)
            self.history['processed_dates'].sort()
            
            # Remove from failed dates if it was there
            if date in self.history['failed_dates']:
                self.history['failed_dates'].remove(date)
            
            # Update last successful date if this is the latest
            if (self.history['last_successful_date'] is None or 
                date > self.history['last_successful_date']):
                self.history['last_successful_date'] = date
            
            self.history['last_update'] = datetime.now().isoformat()
            self._save_history()
            print(f"✅ Marked {date} as processed")
        else:
            print(f"ℹ️  {date} was already marked as processed")
