"""
Data loading functions for GTFS and processed transit data.
Fixed to avoid mixed type warnings.
"""
import pandas as pd
import numpy as np
import warnings
import os
import zipfile
import tempfile
from typing import Tuple, Optional

from .config import PathManager, Config


def load_gtfs_data(date: str, raw_data_folder: Optional[str] = None, print_shapes: bool = False) -> Tuple[pd.DataFrame, ...]:
    """
    Load GTFS data files for a specific date.
    Supports both folder structure and zip files.
    
    Args:
        date: Date string (e.g., '20131018')
        raw_data_folder: Custom raw data folder path. If None, uses default.
        print_shapes: Whether to print DataFrame shapes
        
    Returns:
        Tuple of DataFrames: (routes, trips, shapes, calendar, calendar_dates)
    """
    # Use custom raw data folder or auto-detect
    if raw_data_folder is None:
        base_raw_folder = Config.get_default_raw_data_folder()
    else:
        base_raw_folder = raw_data_folder
    
    # Check if we have a folder or zip file
    base_path = os.path.join(base_raw_folder, date)
    folder_path = base_path
    zip_path = base_path + '.zip'
    
    temp_dir = None
    cleanup_needed = False
    
    try:
        if os.path.isdir(folder_path):
            # Use existing folder
            data_path = folder_path
        elif os.path.isfile(zip_path):
            # Extract zip file to temporary directory
            temp_dir = tempfile.mkdtemp()
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            data_path = temp_dir
            cleanup_needed = True
        else:
            # Neither folder nor zip exists
            raise FileNotFoundError(f"No data found for date {date}. Checked: {folder_path} and {zip_path}")
        
        # Get file paths from the data directory
        routes_path = os.path.join(data_path, Config.GTFS_ROUTES_FILE)
        trips_path = os.path.join(data_path, Config.GTFS_TRIPS_FILE)
        shapes_path = os.path.join(data_path, Config.GTFS_SHAPES_FILE)
        calendar_path = os.path.join(data_path, Config.GTFS_CALENDAR_FILE)
        calendar_dates_path = os.path.join(data_path, Config.GTFS_CALENDAR_DATES_FILE)
        
        # Load the data with proper dtype handling
        routes_df = pd.read_csv(routes_path)
        
        # Read trips with mixed type handling - suppress warnings for cleaner output
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", pd.errors.DtypeWarning)
            trips_df = pd.read_csv(trips_path, dtype={
                'service_id': 'str',
                'trip_id': 'str', 
                'route_id': 'str',
                'shape_id': 'str',
                'trip_headsign': 'str',
                'direction_id': 'Int64',  # nullable integer
                'block_id': 'str'
            }, low_memory=False)
        
        # Read shapes with mixed type handling
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", pd.errors.DtypeWarning)
            shapes_df = pd.read_csv(shapes_path, dtype={
                'shape_id': 'str',
                'shape_pt_lat': 'float64',
                'shape_pt_lon': 'float64', 
                'shape_pt_sequence': 'Int64',
                'shape_dist_traveled': 'float64'
            }, low_memory=False)
        
        # Read calendar_dates with proper types
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", pd.errors.DtypeWarning)
            calendar_dates_df = pd.read_csv(calendar_dates_path, dtype={
                'service_id': 'str',
                'date': 'str',  # Will be converted to datetime later
                'exception_type': 'Int64'
            }, low_memory=False)

        if print_shapes:
            print("Routes:", routes_df.shape)
            print("Trips:", trips_df.shape)
            print("Shapes:", shapes_df.shape)
            print("Calendar Dates:", calendar_dates_df.shape)

        try:
            calendar_df = pd.read_csv(calendar_path, parse_dates=['start_date', 'end_date'])
        except FileNotFoundError:
            if print_shapes:
                print("Calendar file not found. Creating empty dataframe.")
            calendar_df = _create_empty_calendar_dataframe()
        
        return routes_df, trips_df, shapes_df, calendar_df, calendar_dates_df
    
    finally:
        # Clean up temporary directory if we created one
        if cleanup_needed and temp_dir and os.path.exists(temp_dir):
            import shutil
            shutil.rmtree(temp_dir)


def load_processed_data(data_folder: Optional[str] = None) -> Tuple[pd.DataFrame, ...]:
    """
    Load processed data files or create empty ones if they don't exist.
    
    Args:
        data_folder: Custom data folder path. If None, uses auto-detected path.
        
    Returns:
        Tuple of DataFrames: (shapes, routes, route_versions, shape_variants, 
                             shape_variant_activations, temporary_changes)
    """
    if data_folder is None:
        data_folder = Config.get_default_processed_data_folder()
        
    file_paths = PathManager.get_processed_data_paths(data_folder)
    
    try:
        # Load shapes with proper dtype handling to avoid mixed type warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", pd.errors.DtypeWarning)
            shapes_df = pd.read_csv(file_paths[0], dtype={
                'shape_id': 'str',
                'shape_pt_lat': 'float64',
                'shape_pt_lon': 'float64',
                'shape_pt_sequence': 'Int64',
                'shape_dist_traveled': 'float64',
                'shape_bkk_ref': 'str'  # This column can have mixed types, force to string
            }, low_memory=False)
        
        # Load routes
        routes_df = pd.read_csv(file_paths[1], dtype={
            'route_id': 'str',
            'agency_id': 'str',
            'route_short_name': 'str',
            'route_type': 'Int64',
            'route_color': 'str',
            'route_text_color': 'str'
        })
        
        # Load route versions with date parsing
        route_versions_df = pd.read_csv(file_paths[2], parse_dates=['valid_from', 'valid_to'], dtype={
            'version_id': 'Int64',
            'route_id': 'str',
            'direction_id': 'Int64',
            'route_long_name': 'str',
            'route_desc': 'str',
            'main_shape_id': 'str',
            'trip_headsign': 'str',
            'parent_version_id': 'Int64',
            'note': 'str'
        })
        
        # Load shape variants
        shape_variants_df = pd.read_csv(file_paths[3], dtype={
            'shape_variant_id': 'Int64',
            'version_id': 'Int64',
            'shape_id': 'str',
            'trip_headsign': 'str',
            'is_main': 'Int64',
            'note': 'str'
        })
        
        # Load shape variant activations
        shape_variant_activations_df = pd.read_csv(file_paths[4], dtype={
            'date': 'str',
            'shape_variant_id': 'Int64',
            'exception_type': 'float64'
        })
        
        # Load temporary changes
        temporary_changes_df = pd.read_csv(file_paths[5], dtype={
            'detour_id': 'str',
            'route_id': 'str',
            'start_date': 'str',
            'end_date': 'str',
            'affects_version_id': 'Int64',
            'description': 'str'
        })
        
    except FileNotFoundError:
        print("Some processed data files not found. Creating empty dataframes.")
        (shapes_df, routes_df, route_versions_df, shape_variants_df, 
         shape_variant_activations_df, temporary_changes_df) = _create_empty_processed_dataframes()
        
        # Save empty dataframes
        _save_empty_dataframes(
            file_paths, shapes_df, routes_df, route_versions_df, 
            shape_variants_df, shape_variant_activations_df, temporary_changes_df
        )

    return (shapes_df, routes_df, route_versions_df, shape_variants_df, 
            shape_variant_activations_df, temporary_changes_df)


def _create_empty_calendar_dataframe() -> pd.DataFrame:
    """Create an empty calendar DataFrame with proper structure."""
    calendar_df = pd.DataFrame(columns=[
        'service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 
        'friday', 'saturday', 'sunday', 'start_date', 'end_date'
    ])
    calendar_df['start_date'] = pd.to_datetime(calendar_df['start_date'])
    calendar_df['end_date'] = pd.to_datetime(calendar_df['end_date'])
    return calendar_df


def _create_empty_processed_dataframes() -> Tuple[pd.DataFrame, ...]:
    """Create empty processed DataFrames with proper structure and dtypes."""
    
    # shapes_df with proper dtypes
    shapes_df = pd.DataFrame(columns=[
        "shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence", 
        "shape_dist_traveled", "shape_bkk_ref"
    ]).astype({
        'shape_id': 'str',
        'shape_pt_lat': 'float64',
        'shape_pt_lon': 'float64',
        'shape_pt_sequence': 'Int64',
        'shape_dist_traveled': 'float64',
        'shape_bkk_ref': 'str'
    })

    # routes_df with proper dtypes
    routes_df = pd.DataFrame(columns=[
        "route_id", "agency_id", "route_short_name", "route_type", 
        "route_color", "route_text_color"
    ]).astype({
        'route_id': 'str',
        'agency_id': 'str',
        'route_short_name': 'str',
        'route_type': 'Int64',
        'route_color': 'str',
        'route_text_color': 'str'
    })

    # route_versions_df with proper dtypes
    route_versions_df = pd.DataFrame(columns=[
        "version_id", "route_id", "direction_id", "route_long_name", "route_desc",
        "valid_from", "valid_to", "main_shape_id", "trip_headsign",
        "parent_version_id", "note"
    ])
    route_versions_df = route_versions_df.astype({
        'version_id': 'Int64',
        'route_id': 'str',
        'direction_id': 'Int64',
        'route_long_name': 'str',
        'route_desc': 'str',
        'main_shape_id': 'str',
        'trip_headsign': 'str',
        'parent_version_id': 'Int64',
        'note': 'str'
    })
    route_versions_df['valid_from'] = pd.to_datetime(route_versions_df['valid_from'])
    route_versions_df['valid_to'] = pd.to_datetime(route_versions_df['valid_to'])
    
    # shape_variants_df with proper dtypes
    shape_variants_df = pd.DataFrame(columns=[
        "shape_variant_id", "version_id", "shape_id", "trip_headsign", "is_main", "note"
    ]).astype({
        'shape_variant_id': 'Int64',
        'version_id': 'Int64',
        'shape_id': 'str',
        'trip_headsign': 'str',
        'is_main': 'Int64',
        'note': 'str'
    })

    # shape_variant_activations_df with proper dtypes
    shape_variant_activations_df = pd.DataFrame(columns=[
        "date", "shape_variant_id", "exception_type"
    ]).astype({
        'date': 'str',
        'shape_variant_id': 'Int64',
        'exception_type': 'float64'
    })

    # temporary_changes_df with proper dtypes
    temporary_changes_df = pd.DataFrame(columns=[
        "detour_id", "route_id", "start_date", "end_date", "affects_version_id", "description"
    ]).astype({
        'detour_id': 'str',
        'route_id': 'str',
        'start_date': 'str',
        'end_date': 'str',
        'affects_version_id': 'Int64',
        'description': 'str'
    })
    
    return (shapes_df, routes_df, route_versions_df, shape_variants_df, 
            shape_variant_activations_df, temporary_changes_df)


def _save_empty_dataframes(file_paths: Tuple[str, ...], *dataframes: pd.DataFrame) -> None:
    """Save empty dataframes to their respective files."""
    # Ensure directory exists
    os.makedirs(os.path.dirname(file_paths[0]), exist_ok=True)
    
    for path, df in zip(file_paths, dataframes):
        df.to_csv(path, index=False)