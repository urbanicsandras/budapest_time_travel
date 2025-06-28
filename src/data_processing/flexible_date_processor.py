"""
Improved date processing with flexible input options.
Supports single dates, date ranges, and custom date lists.
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union
from data_processing import TransitDataProcessor


class FlexibleDateProcessor:
    """Enhanced processor with flexible date input options."""
    
    def __init__(self, data_folder: str = '../data/processed/'):
        """Initialize the processor with data folder."""
        self.processor = TransitDataProcessor(data_folder)
        self.data_folder = data_folder
    
    def process_dates(self, dates: Union[str, List[str], Dict[str, str]], 
                     save_data: bool = True, 
                     show_progress: bool = True) -> Dict[str, Dict]:
        """
        Flexible date processing method that handles various input types.
        
        Args:
            dates: Can be:
                - Single date string: '20131018'
                - List of dates: ['20131018', '20131019']
                - Date range dict: {'start': '20131018', 'end': '20131025'}
                - Date range dict with days: {'start': '20131018', 'days': 7}
            save_data: Whether to save processed data
            show_progress: Whether to show progress messages
            
        Returns:
            Dictionary with processing results
        """
        # Parse input and get list of dates to process
        dates_to_process = self._parse_date_input(dates)
        
        if not dates_to_process:
            print("No valid dates to process.")
            return {}
        
        if show_progress:
            print(f"Processing {len(dates_to_process)} date(s): {dates_to_process[0]} to {dates_to_process[-1]}")
        
        return self._process_date_list(dates_to_process, save_data, show_progress)
    
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
                          show_progress: bool) -> Dict[str, Dict]:
        """Process a list of dates with error handling and progress tracking."""
        results = {}
        total_dates = len(dates)
        
        for i, date in enumerate(dates, 1):
            if show_progress:
                print(f"\n--- Processing {date} ({i}/{total_dates}) ---")
            
            try:
                result = self.processor.process_date(date, save_data=save_data)
                results[date] = {
                    'status': 'success',
                    'data': result,
                    'error': None
                }
                if show_progress:
                    print(f"✓ Successfully processed {date}")
                    
            except Exception as e:
                results[date] = {
                    'status': 'failed',
                    'data': None,
                    'error': str(e)
                }
                if show_progress:
                    print(f"✗ Failed to process {date}: {e}")
        
        # Summary
        successful = sum(1 for r in results.values() if r['status'] == 'success')
        failed = total_dates - successful
        
        if show_progress:
            print(f"\n=== PROCESSING SUMMARY ===")
            print(f"Total dates: {total_dates}")
            print(f"Successful: {successful}")
            print(f"Failed: {failed}")
            
            if failed > 0:
                failed_dates = [date for date, r in results.items() if r['status'] == 'failed']
                print(f"Failed dates: {failed_dates}")
        
        return results


# Convenience functions for easy usage
def process_single_date(date: str, data_folder: str = '../data/processed/', 
                       save_data: bool = True) -> Dict:
    """
    Process a single date.
    
    Args:
        date: Date string in YYYYMMDD format
        data_folder: Path to processed data folder
        save_data: Whether to save results
        
    Returns:
        Processing result dictionary
    """
    processor = FlexibleDateProcessor(data_folder)
    results = processor.process_dates(date, save_data=save_data)
    return results.get(date, {})


def process_date_range(start_date: str, end_date: str = None, days: int = None,
                      data_folder: str = '../data/processed/', 
                      save_data: bool = True) -> Dict[str, Dict]:
    """
    Process a range of dates.
    
    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format (if not using days)
        days: Number of days to process (if not using end_date)
        data_folder: Path to processed data folder
        save_data: Whether to save results
        
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
    
    return processor.process_dates(date_spec, save_data=save_data)


def process_date_list(dates: List[str], data_folder: str = '../data/processed/',
                     save_data: bool = True) -> Dict[str, Dict]:
    """
    Process a list of dates.
    
    Args:
        dates: List of date strings in YYYYMMDD format
        data_folder: Path to processed data folder
        save_data: Whether to save results
        
    Returns:
        Dictionary with results for each date
    """
    processor = FlexibleDateProcessor(data_folder)
    return processor.process_dates(dates, save_data=save_data)


# ================================
# USAGE EXAMPLES
# ================================

if __name__ == "__main__":
    # Initialize the flexible processor
    processor = FlexibleDateProcessor('../data/processed/')
    
    print("=== EXAMPLE 1: Single Date ===")
    # Process single date
    result1 = processor.process_dates('20131018')
    
    print("\n=== EXAMPLE 2: Date List ===")
    # Process list of dates
    result2 = processor.process_dates(['20131018', '20131019', '20131020'])
    
    print("\n=== EXAMPLE 3: Date Range with End Date ===")
    # Process date range with start and end
    result3 = processor.process_dates({
        'start': '20131018',
        'end': '20131025'
    })
    
    print("\n=== EXAMPLE 4: Date Range with Number of Days ===")
    # Process date range with start and number of days
    result4 = processor.process_dates({
        'start': '20131018',
        'days': 7
    })
    
    print("\n=== EXAMPLE 5: Using Convenience Functions ===")
    
    # Single date
    single_result = process_single_date('20131018')
    
    # Date range
    range_result = process_date_range('20131018', end_date='20131020')
    
    # Or with days
    range_result2 = process_date_range('20131018', days=3)
    
    # Date list
    list_result = process_date_list(['20131018', '20131019', '20131020'])
    
    print("\n=== EXAMPLE 6: Advanced Usage ===")
    
    # Process without saving (for testing)
    test_result = processor.process_dates('20131018', save_data=False)
    
    # Process without progress messages
    quiet_result = processor.process_dates({
        'start': '20131018', 
        'days': 3
    }, show_progress=False)
    
    # Error handling example
    try:
        # This will handle invalid dates gracefully
        result = processor.process_dates(['20131018', 'invalid_date', '20131019'])
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n=== EXAMPLE 7: Real-world Usage Patterns ===")
    
    # Process a month of data
    month_result = process_date_range('20131001', '20131031')
    
    # Process a week starting from a specific date
    week_result = process_date_range('20131018', days=7)
    
    # Process specific dates (e.g., only weekdays)
    weekdays = ['20131021', '20131022', '20131023', '20131024', '20131025']  # Mon-Fri
    weekday_result = process_date_list(weekdays)
    
    # Process and get detailed results
    detailed_results = processor.process_dates({
        'start': '20131018',
        'days': 5
    })
    
    # Extract successful results
    successful_dates = [date for date, info in detailed_results.items() 
                       if info['status'] == 'success']
    print(f"Successfully processed dates: {successful_dates}")
    
    # Extract and analyze data from successful processing
    for date in successful_dates:
        data = detailed_results[date]['data']
        routes_count = len(data['routes'])
        print(f"Date {date}: {routes_count} routes processed")
