"""
Configuration and path management for transit data processing.
"""
import os
from typing import Tuple, Optional


class Config:
    """Configuration settings for the transit data processing pipeline."""
    
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
    
    @staticmethod
    def find_project_root():
        """Find the project root directory by looking for the data folder."""
        current_dir = os.getcwd()
        
        # Look for data folder in current directory and parent directories
        while current_dir != os.path.dirname(current_dir):  # Until we reach filesystem root
            data_path = os.path.join(current_dir, 'data')
            if os.path.exists(data_path):
                return current_dir
            current_dir = os.path.dirname(current_dir)
        
        # If not found, assume current directory
        return os.getcwd()
    
    @staticmethod
    def get_default_raw_data_folder():
        """Get the default raw data folder path."""
        project_root = Config.find_project_root()
        return os.path.join(project_root, 'data', 'raw')
    
    @staticmethod
    def get_default_processed_data_folder():
        """Get the default processed data folder path."""
        project_root = Config.find_project_root()
        return os.path.join(project_root, 'data', 'processed')


class PathManager:
    """Manages file paths for the transit data processing pipeline."""
    
    @staticmethod
    def get_processed_data_paths(data_folder: Optional[str] = None) -> Tuple[str, ...]:
        """
        Get paths for processed data files.
        
        Args:
            data_folder: Custom data folder path. If None, uses auto-detected path.
            
        Returns:
            Tuple of file paths for processed data files.
        """
        if data_folder is None:
            data_folder = Config.get_default_processed_data_folder()
            
        return (
            os.path.join(data_folder, Config.SHAPES_FILE),
            os.path.join(data_folder, Config.ROUTES_FILE),
            os.path.join(data_folder, Config.ROUTE_VERSIONS_FILE),
            os.path.join(data_folder, Config.SHAPE_VARIANTS_FILE),
            os.path.join(data_folder, Config.SHAPE_VARIANT_ACTIVATIONS_FILE),
            os.path.join(data_folder, Config.TEMPORARY_CHANGES_FILE)
        )
    
    @staticmethod
    def get_gtfs_data_paths(date: str, raw_data_folder: Optional[str] = None) -> Tuple[str, ...]:
        """
        Get paths for GTFS data files for a specific date.
        
        Args:
            date: Date string (e.g., '20131018')
            raw_data_folder: Custom raw data folder path. If None, uses auto-detected path.
            
        Returns:
            Tuple of file paths for GTFS data files.
        """
        if raw_data_folder is None:
            raw_data_folder = Config.get_default_raw_data_folder()
            
        date_folder = os.path.join(raw_data_folder, date)
        
        return (
            os.path.join(date_folder, Config.GTFS_ROUTES_FILE),
            os.path.join(date_folder, Config.GTFS_TRIPS_FILE),
            os.path.join(date_folder, Config.GTFS_SHAPES_FILE),
            os.path.join(date_folder, Config.GTFS_CALENDAR_FILE),
            os.path.join(date_folder, Config.GTFS_CALENDAR_DATES_FILE)
        )