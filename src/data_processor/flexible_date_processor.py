"""
Enhanced FlexibleDateProcessor with detailed progress control options.
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union
from .pipeline import TransitDataProcessor


class FlexibleDateProcessor:
    """Enhanced processor with flexible date input and progress control options."""
    
    def __init__(self, data_folder: str = '../data/processed/'):
        """Initialize the processor with data folder."""
        self.processor = TransitDataProcessor(data_folder)
        self.data_folder = data_folder
    
    def process_dates(self, dates: Union[str, List[str], Dict[str, str]], 
                     save_data: bool = True, 
                     progress: Union[bool, str] = True,
                     return_data: bool = False) -> Dict[str, Dict]:
        """
        Flexible date processing method that handles various input types.
        
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
            
        Returns:
            Dictionary with processing results. If return_data=False, only contains
            status information. If return_data=True, includes all processed DataFrames.
        """
        # Parse input and get list of dates to process
        dates_to_process = self._parse_date_input(dates)
        
        if not dates_to_process:
            if progress not in [False, 'none']:
                print("No valid dates to process.")
            return {}
        
        if progress not in [False, 'none']:
            print(f"Processing {len(dates_to_process)} date(s): {dates_to_process[0]} to {dates_to_process[-1]}")
        
        return self._process_date_list(dates_to_process, save_data, progress, return_data)
    
    def _parse_date_input(self, dates_input: Union[str, List[str], Dict[str, str]]) -> List[str]:
        """Parse various date input formats into a list of date strings."""
        
        if isinstance(dates_input, str):
            # Single date string
            if self._is_valid_date_string(dates_input):
                return [dates_input]
            else:
                print(f"Invalid date format: {dates_input}. Expected YYYYMMDD format.")
                return []
        
        elif isinstance(dates_input, list):
            # List of dates
            valid_dates = []
            for date in dates_input:
                if self._is_valid_date_string(date):
                    valid_dates.append(date)
                else:
                    print(f"Skipping invalid date: {date}")
            return valid_dates
        
        elif isinstance(dates_input, dict):
            # Date range dictionary
            return self._parse_date_range(dates_input)
        
        else:
            print(f"Unsupported date input type: {type(dates_input)}")
            return []
    
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


# Enhanced convenience functions with progress control
def process_single_date(date: str, data_folder: str = '../data/processed/', 
                       save_data: bool = True, return_data: bool = False,
                       progress: Union[bool, str] = True) -> Dict:
    """
    Process a single date with progress control.
    
    Args:
        date: Date string in YYYYMMDD format
        data_folder: Path to processed data folder
        save_data: Whether to save results
        return_data: Whether to return the processed DataFrames
        progress: Progress display option
        
    Returns:
        Processing result dictionary
    """
    processor = FlexibleDateProcessor(data_folder)
    results = processor.process_dates(date, save_data=save_data, return_data=return_data, progress=progress)
    return results.get(date, {})


def process_date_range(start_date: str, end_date: str = None, days: int = None,
                      data_folder: str = '../data/processed/', 
                      save_data: bool = True, return_data: bool = False,
                      progress: Union[bool, str] = True) -> Dict[str, Dict]:
    """
    Process a range of dates with progress control.
    
    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format (if not using days)
        days: Number of days to process (if not using end_date)
        data_folder: Path to processed data folder
        save_data: Whether to save results
        return_data: Whether to return the processed DataFrames
        progress: Progress display option
        
    Returns:
        Dictionary with results for each date
    """
    processor = FlexibleDateProcessor(data_folder)
    
    if end_date:
        date_spec = {'start': start_date, 'end': end_date}
    elif days:
        date_spec = {'start': start_date, 'days': days}
    else:
        raise ValueError("Must specify either end_date or days")
    
    return processor.process_dates(date_spec, save_data=save_data, return_data=return_data, progress=progress)


def process_date_list(dates: List[str], data_folder: str = '../data/processed/',
                     save_data: bool = True, return_data: bool = False,
                     progress: Union[bool, str] = True) -> Dict[str, Dict]:
    """
    Process a list of dates with progress control.
    
    Args:
        dates: List of date strings in YYYYMMDD format
        data_folder: Path to processed data folder
        save_data: Whether to save results
        return_data: Whether to return the processed DataFrames
        progress: Progress display option
        
    Returns:
        Dictionary with results for each date
    """
    processor = FlexibleDateProcessor(data_folder)
    return processor.process_dates(dates, save_data=save_data, return_data=return_data, progress=progress)


# ================================
# USAGE EXAMPLES FOR DIFFERENT PROGRESS LEVELS
# ================================

if __name__ == "__main__":
    processor = FlexibleDateProcessor('../data/processed/')
    
    print("=== PROGRESS OPTION EXAMPLES ===")
    
    # 1. No progress output (silent processing)
    print("\n1. Silent processing (progress=False):")
    result1 = processor.process_dates(['20131018', '20131019'], progress=False)
    
    print("\n2. Silent processing (progress='none'):")
    result2 = processor.process_dates(['20131018', '20131019'], progress='none')
    
    # 3. Full detailed progress (default)
    print("\n3. Full progress (progress=True or 'full'):")
    result3 = processor.process_dates(['20131018', '20131019'], progress='full')
    
    # 4. Minimal progress - only date headers
    print("\n4. Minimal progress (progress='minimal'):")
    result4 = processor.process_dates(['20131018', '20131019'], progress='minimal')
    
    # 5. Compact progress - one line per date
    print("\n5. Compact progress (progress='compact'):")
    result5 = processor.process_dates(['20131018', '20131019'], progress='compact')
    
    # 6. Summary only - just final results
    print("\n6. Summary only (progress='summary'):")
    result6 = processor.process_dates(['20131018', '20131019'], progress='summary')
    
    print("\n=== REAL-WORLD USAGE SCENARIOS ===")
    
    # For large batch processing - use silent or compact
    print("\nLarge batch processing (month of data):")
    processor.process_dates({'start': '20131001', 'end': '20131031'}, progress='compact')
    
    # For debugging or detailed analysis - use full
    print("\nDetailed analysis:")
    processor.process_dates(['20131018', '20131019'], progress='full')
    
    # For automation scripts - use none
    print("\nAutomation script:")
    processor.process_dates(['20131018'], progress='none')
    
    # For monitoring long-running jobs - use minimal
    print("\nMonitoring long jobs:")
    processor.process_dates({'start': '20131018', 'days': 7}, progress='minimal')