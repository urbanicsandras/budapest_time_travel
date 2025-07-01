"""
Enhanced FlexibleDateProcessor with detailed progress control options and processing tracking.
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union
from .pipeline import TransitDataProcessor
from .processing_tracker import ProcessingTracker


class FlexibleDateProcessor:
    """Enhanced processor with flexible date input, progress control, and processing tracking."""
    
    def __init__(self, data_folder: str = None, raw_data_folder: str = None, use_tracker: bool = True):
        """
        Initialize the processor with data folder.
        
        Args:
            data_folder: Path to processed data folder. If None, auto-detects project structure.
            raw_data_folder: Path to raw data folder. If None, auto-detects project structure.
            use_tracker: Whether to use processing history tracking for smart resuming.
        """
        self.processor = TransitDataProcessor(data_folder, raw_data_folder)
        self.data_folder = data_folder
        self.raw_data_folder = raw_data_folder
        self.use_tracker = use_tracker
        
        if use_tracker:
            self.tracker = ProcessingTracker(data_folder)
    
    def process_dates(self, dates: Union[str, List[str], Dict[str, str]], 
                     save_data: bool = True, 
                     progress: Union[bool, str] = True,
                     return_data: bool = False,
                     smart_resume: bool = True) -> Dict[str, Dict]:
        """
        Flexible date processing method that handles various input types with smart resuming.
        
        Args:
            dates: Can be:
                - Single date string: '20131018'
                - List of dates: ['20131018', '20131019']
                - Date range dict: {'start': '20131018', 'end': '20131025'}
                - Date range dict with days: {'start': '20131018', 'days': 7}
            save_data: Whether to save processed data
            progress: Progress display options:
                - False or 'none': No progress output
                - True or 'full': Full detailed progress (default)
                - 'minimal': Only date processing headers
                - 'summary': Only final summary
                - 'compact': One line per date with status
            return_data: Whether to return the processed DataFrames for each date
            smart_resume: Whether to automatically skip already processed dates (only works with date ranges)
            
        Returns:
            Dictionary with processing results. If return_data=False, only contains
            status information. If return_data=True, includes all processed DataFrames.
        """
        # Parse input and get list of dates to process
        dates_to_process, original_range = self._parse_date_input_with_tracking(dates, smart_resume)
        
        if not dates_to_process:
            if progress not in [False, 'none']:
                print("No dates need processing.")
            return {}
        
        if progress not in [False, 'none']:
            print(f"Processing {len(dates_to_process)} date(s): {dates_to_process[0]} to {dates_to_process[-1]}")
        
        # Process the dates
        results = self._process_date_list(dates_to_process, save_data, progress, return_data)
        
        # Record processing session if using tracker
        if self.use_tracker and original_range:
            successful_dates = [date for date, info in results.items() if info['status'] == 'success']
            failed_dates = [date for date, info in results.items() if info['status'] == 'failed']
            
            self.tracker.record_processing_session(
                original_range['start'], 
                original_range['end'], 
                successful_dates, 
                failed_dates
            )
        
        return results
    
    def _parse_date_input_with_tracking(self, dates_input: Union[str, List[str], Dict[str, str]], 
                                       smart_resume: bool) -> tuple[List[str], Optional[Dict[str, str]]]:
        """Parse date input and apply smart resuming if enabled."""
        
        original_range = None
        
        if isinstance(dates_input, str):
            # Single date string
            if self._is_valid_date_string(dates_input):
                return [dates_input], None
            else:
                print(f"Invalid date format: {dates_input}. Expected YYYYMMDD format.")
                return [], None
        
        elif isinstance(dates_input, list):
            # List of dates - no smart resuming for lists
            valid_dates = []
            for date in dates_input:
                if self._is_valid_date_string(date):
                    valid_dates.append(date)
                else:
                    print(f"Skipping invalid date: {date}")
            return valid_dates, None
        
        elif isinstance(dates_input, dict):
            # Date range dictionary - apply smart resuming if enabled
            date_range = self._parse_date_range(dates_input)
            if not date_range:
                return [], None
            
            # Determine original range for tracking
            if 'start' in dates_input:
                start_date = dates_input['start']
                if 'end' in dates_input:
                    end_date = dates_input['end']
                elif 'days' in dates_input:
                    try:
                        start = datetime.strptime(start_date, '%Y%m%d')
                        end = start + timedelta(days=int(dates_input['days']) - 1)
                        end_date = end.strftime('%Y%m%d')
                    except (ValueError, TypeError):
                        end_date = start_date
                else:
                    end_date = start_date
                
                original_range = {'start': start_date, 'end': end_date}
            
            # Apply smart resuming if enabled and using tracker
            if smart_resume and self.use_tracker and original_range:
                dates_to_process = self.tracker.get_dates_to_process(
                    original_range['start'], 
                    original_range['end'], 
                    self.raw_data_folder
                )
                return dates_to_process, original_range
            else:
                return date_range, original_range
        
        else:
            print(f"Unsupported date input type: {type(dates_input)}")
            return [], None
    
    def get_processing_summary(self) -> None:
        """Print processing history summary."""
        if self.use_tracker:
            self.tracker.print_summary()
        else:
            print("Processing tracking is disabled.")
    
    def reset_processing_history(self) -> None:
        """Reset processing history (use with caution!)."""
        if self.use_tracker:
            self.tracker.reset_history()
        else:
            print("Processing tracking is disabled.")
    
    def mark_date_as_processed(self, date: str) -> None:
        """Manually mark a date as processed."""
        if self.use_tracker:
            self.tracker.mark_date_as_processed(date)
        else:
            print("Processing tracking is disabled.")
    
    def _is_valid_date_string(self, date_str: str) -> bool:
        """Check if a string is a valid date in YYYYMMDD format."""
        try:
            datetime.strptime(date_str, '%Y%m%d')
            return True
        except ValueError:
            return False
    
    def _parse_date_range(self, date_range: Dict[str, str]) -> List[str]:
        """Parse date range dictionary into list of dates."""
        if 'start' not in date_range:
            print("Date range must contain 'start' key.")
            return []
        
        start_date = date_range['start']
        if not self._is_valid_date_string(start_date):
            print(f"Invalid start date: {start_date}")
            return []
        
        start = datetime.strptime(start_date, '%Y%m%d')
        
        # Determine end date
        if 'end' in date_range:
            end_date = date_range['end']
            if not self._is_valid_date_string(end_date):
                print(f"Invalid end date: {end_date}")
                return []
            end = datetime.strptime(end_date, '%Y%m%d')
        
        elif 'days' in date_range:
            try:
                num_days = int(date_range['days'])
                end = start + timedelta(days=num_days - 1)
            except (ValueError, TypeError):
                print(f"Invalid days value: {date_range['days']}")
                return []
        
        else:
            print("Date range must contain either 'end' or 'days' key.")
            return []
        
        # Generate date list
        if start > end:
            print("Start date must be before or equal to end date.")
            return []
        
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime('%Y%m%d'))
            current += timedelta(days=1)
        
        return dates
    
    def _process_date_list(self, dates: List[str], save_data: bool, 
                          progress: Union[bool, str], return_data: bool) -> Dict[str, Dict]:
        """Process a list of dates with configurable progress tracking."""
        results = {}
        total_dates = len(dates)
        
        # Configure progress settings
        show_headers = progress in [True, 'full', 'minimal']
        show_details = progress in [True, 'full']
        show_compact = progress == 'compact'
        show_summary = progress in [True, 'full', 'summary']
        
        for i, date in enumerate(dates, 1):
            # Show date processing header
            if show_headers:
                print(f"\n--- Processing {date} ({i}/{total_dates}) ---")
            elif show_compact:
                print(f"Processing {date} ({i}/{total_dates})... ", end='', flush=True)
            
            try:
                # Control internal processor progress based on our progress setting
                show_internal_progress = progress in [True, 'full']
                
                result = self.processor.process_date(date, save_data=save_data, return_data=return_data, 
                                                   show_progress=show_internal_progress)
                
                results[date] = {
                    'status': 'success',
                    'data': result if return_data else None,
                    'error': None
                }
                
                if show_details:
                    print(f"✓ Successfully processed {date}")
                elif show_compact:
                    print("✓")
                    
            except Exception as e:
                results[date] = {
                    'status': 'failed',
                    'data': None,
                    'error': str(e)
                }
                
                if show_details:
                    print(f"✗ Failed to process {date}: {e}")
                elif show_compact:
                    print(f"✗ ({str(e)[:50]}...)" if len(str(e)) > 50 else f"✗ ({e})")
        
        # Show summary
        if show_summary:
            successful = sum(1 for r in results.values() if r['status'] == 'success')
            failed = total_dates - successful
            
            print(f"\n=== PROCESSING SUMMARY ===")
            print(f"Total dates: {total_dates}")
            print(f"Successful: {successful}")
            print(f"Failed: {failed}")
            
            if failed > 0:
                failed_dates = [date for date, r in results.items() if r['status'] == 'failed']
                if progress in [True, 'full']:
                    print(f"Failed dates: {failed_dates}")
                else:
                    print(f"Failed dates: {len(failed_dates)} dates")
        
        return results


# Enhanced convenience functions with progress and tracking control
def process_single_date(date: str, data_folder: str = None, 
                       save_data: bool = True, return_data: bool = False,
                       progress: Union[bool, str] = True, use_tracker: bool = True) -> Dict:
    """
    Process a single date with progress control and optional tracking.
    
    Args:
        date: Date string in YYYYMMDD format
        data_folder: Path to processed data folder
        save_data: Whether to save results
        return_data: Whether to return the processed DataFrames
        progress: Progress display option
        use_tracker: Whether to use processing tracking
        
    Returns:
        Processing result dictionary
    """
    processor = FlexibleDateProcessor(data_folder, use_tracker=use_tracker)
    results = processor.process_dates(date, save_data=save_data, return_data=return_data, progress=progress)
    return results.get(date, {})


def process_date_range(start_date: str, end_date: str = None, days: int = None,
                      data_folder: str = None, 
                      save_data: bool = True, return_data: bool = False,
                      progress: Union[bool, str] = True, use_tracker: bool = True,
                      smart_resume: bool = True) -> Dict[str, Dict]:
    """
    Process a range of dates with progress control and smart resuming.
    
    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format (if not using days)
        days: Number of days to process (if not using end_date)
        data_folder: Path to processed data folder
        save_data: Whether to save results
        return_data: Whether to return the processed DataFrames
        progress: Progress display option
        use_tracker: Whether to use processing tracking
        smart_resume: Whether to automatically skip already processed dates
        
    Returns:
        Dictionary with results for each date
    """
    processor = FlexibleDateProcessor(data_folder, use_tracker=use_tracker)
    
    if end_date:
        date_spec = {'start': start_date, 'end': end_date}
    elif days:
        date_spec = {'start': start_date, 'days': days}
    else:
        raise ValueError("Must specify either end_date or days")
    
    return processor.process_dates(date_spec, save_data=save_data, return_data=return_data, 
                                 progress=progress, smart_resume=smart_resume)


def process_date_list(dates: List[str], data_folder: str = None,
                     save_data: bool = True, return_data: bool = False,
                     progress: Union[bool, str] = True, use_tracker: bool = True) -> Dict[str, Dict]:
    """
    Process a list of dates with progress control and optional tracking.
    
    Args:
        dates: List of date strings in YYYYMMDD format
        data_folder: Path to processed data folder
        save_data: Whether to save results
        return_data: Whether to return the processed DataFrames
        progress: Progress display option
        use_tracker: Whether to use processing tracking
        
    Returns:
        Dictionary with results for each date
    """
    processor = FlexibleDateProcessor(data_folder, use_tracker=use_tracker)
    return processor.process_dates(dates, save_data=save_data, return_data=return_data, progress=progress)