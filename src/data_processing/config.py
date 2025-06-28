"""
Configuration and path management for transit data processing.
"""
import os
from typing import Tuple, Optional


class Config:
    """Configuration settings for the transit data processing pipeline."""
    
    # Default folders
    RAW_DATA_FOLDER = '../data/raw/'
    PROCESSED_DATA_FOLDER = '../data/processed/'
    
    # File names
    SHAPES_FILE = 'shapes.csv'
    ROUTES_FILE = 'routes.csv'
    ROUTE_VERSIONS_FILE = 'route_versions.csv'
    SHAPE_VARIANTS_FILE = 'shape_variants.csv'
    SHAPE_VARIANT_ACTIVATIONS_FILE = 'shape_variant_activations.csv'
    TEMPORARY_CHANGES_FILE = 'temporary_changes.csv'
    
    # GTFS file names
    GTFS_ROUTES_FILE = 'routes.txt'
    GTFS_TRIPS_FILE = 'trips.txt'
    GTFS_SHAPES_FILE = 'shapes.txt'
    GTFS_CALENDAR_FILE = 'calendar.txt'
    GTFS_CALENDAR_DATES_FILE = 'calendar_dates.txt'
    
    # Starting IDs
    START_VERSION_ID = 100_000
    START_SHAPE_VARIANT_ID = 100_000


class PathManager:
    """Manages file paths for the transit data processing pipeline."""
    
    @staticmethod
    def get_processed_data_paths(data_folder: Optional[str] = None) -> Tuple[str, ...]:
        """
        Get paths for processed data files.
        
        Args:
            data_folder: Custom data folder path. If None, uses default.
            
        Returns:
            Tuple of file paths for processed data files.
        """
        if data_folder is None:
            data_folder = Config.PROCESSED_DATA_FOLDER
            
        return (
            os.path.join(data_folder, Config.SHAPES_FILE),
            os.path.join(data_folder, Config.ROUTES_FILE),
            os.path.join(data_folder, Config.ROUTE_VERSIONS_FILE),
            os.path.join(data_folder, Config.SHAPE_VARIANTS_FILE),
            os.path.join(data_folder, Config.SHAPE_VARIANT_ACTIVATIONS_FILE),
            os.path.join(data_folder, Config.TEMPORARY_CHANGES_FILE)
        )
    
    @staticmethod
    def get_gtfs_data_paths(date: str, data_folder: Optional[str] = None) -> Tuple[str, ...]:
        """
        Get paths for GTFS data files for a specific date.
        
        Args:
            date: Date string (e.g., '20131018')
            data_folder: Custom data folder path. If None, uses default.
            
        Returns:
            Tuple of file paths for GTFS data files.
        """
        if data_folder is None:
            data_folder = Config.RAW_DATA_FOLDER
            
        date_folder = os.path.join(data_folder, date)
        
        return (
            os.path.join(date_folder, Config.GTFS_ROUTES_FILE),
            os.path.join(date_folder, Config.GTFS_TRIPS_FILE),
            os.path.join(date_folder, Config.GTFS_SHAPES_FILE),
            os.path.join(date_folder, Config.GTFS_CALENDAR_FILE),
            os.path.join(date_folder, Config.GTFS_CALENDAR_DATES_FILE)
        )
