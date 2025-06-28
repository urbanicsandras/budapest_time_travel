"""
Date and service utilities for transit data processing.
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional


def get_active_dates(calendar_df: pd.DataFrame, service_id: str, 
                    to_string: bool = True, date_format: str = '%Y-%m-%d') -> List[str]:
    """
    Get all active dates for a service.
    
    Args:
        calendar_df: DataFrame with service data
        service_id: ID of the service to look up
        to_string: Convert dates to strings
        date_format: Format for string conversion
    
    Returns:
        List of all active dates for the service
    """
    # Filter for the specific service_id
    service_row = calendar_df[calendar_df['service_id'] == service_id]
   
    if service_row.empty:
        print(f"Service ID '{service_id}' not found")
        return []
   
    # Get the first (and should be only) row
    service = service_row.iloc[0]
    start_date = pd.to_datetime(service['start_date'], format='%Y%m%d')
    end_date = pd.to_datetime(service['end_date'], format='%Y%m%d')
   
    # Days of week mapping (Monday=0, Sunday=6)
    day_columns = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    days_active = [i for i, day_col in enumerate(day_columns) if service[day_col] == 1]
   
    if not days_active:
        print(f"Service ID '{service_id}' has no active days")
        return []
   
    # Generate all dates in the range that fall on active days
    active_dates = []
    current_date = start_date
   
    while current_date <= end_date:
        # Check if current date's weekday is in our active days
        if current_date.weekday() in days_active:
            active_dates.append(current_date)
        current_date += timedelta(days=1)
   
    if to_string:
        active_dates = [date.strftime(date_format) for date in active_dates]
    return active_dates


def build_service_date_mappings(trips_df: pd.DataFrame, calendar_df: pd.DataFrame) -> tuple[Dict[str, List[str]], Dict[str, Optional[str]]]:
    """
    Build mappings from service IDs to their active dates and first dates.
    
    Args:
        trips_df: DataFrame with trip data
        calendar_df: DataFrame with calendar data
        
    Returns:
        Tuple of (service_dates_dict, service_first_date_dict)
    """
    unique_services = trips_df["service_id"].unique()
    
    trip_dates = {
        service: get_active_dates(calendar_df, service)
        for service in unique_services
    }
    
    trip_first_date = {
        service: dates[0] if dates else None
        for service, dates in trip_dates.items()
    }
    
    return trip_dates, trip_first_date
