"""
Main processing pipeline for transit data processing.
"""
import pandas as pd
from typing import Optional

from .data_loader import load_gtfs_data, load_processed_data
from .date_utils import build_service_date_mappings
from .route_processor import build_latest_routes, update_routes, update_route_versions
from .shape_processor import (
    build_service_data_without_exceptions, 
    build_service_data_with_exceptions,
    build_shape_variant_data,
    update_shape_variants_and_activations
)
from .data_saver import save_routes, save_route_versions, save_shape_variants, save_shape_variant_activations


class TransitDataProcessor:
    """Main class for processing transit data."""
    
    def __init__(self, data_folder: Optional[str] = None):
        """
        Initialize the processor.
        
        Args:
            data_folder: Custom data folder path. If None, uses default.
        """
        self.data_folder = data_folder
        
    def process_date(self, date: str, save_data: bool = True) -> dict:
        """
        Process transit data for a specific date.
        
        Args:
            date: Date string (e.g., '20131018')
            save_data: Whether to save processed data to files
            
        Returns:
            Dictionary containing all processed DataFrames
        """
        print(f"Processing transit data for date: {date}")
        
        # Step 1: Load data
        print("1. Loading GTFS data...")
        routes_txt, trips_txt, shapes_txt, calendar_txt, calendar_dates_txt = load_gtfs_data(date)
        
        print("2. Loading existing processed data...")
        (shapes_df, routes_df, route_versions_df, shape_variants_df, 
         shape_variant_activations_df, temporary_changes_df) = load_processed_data(self.data_folder)
        
        # Step 2: Build service date mappings
        print("3. Building service date mappings...")
        trip_dates, trip_first_date = build_service_date_mappings(trips_txt, calendar_txt)
        
        # Step 3: Process routes
        print("4. Processing routes...")
        latest_routes_df = build_latest_routes(trips_txt, trip_first_date, routes_txt)
        updated_routes_df = update_routes(routes_df, latest_routes_df)
        
        # Step 4: Process route versions
        print("5. Processing route versions...")
        updated_route_versions_df = update_route_versions(route_versions_df, latest_routes_df, date)
        
        # Step 5: Process shape variants
        print("6. Processing shape variants...")
        df_noexceptions = build_service_data_without_exceptions(trip_dates, trips_txt)
        df_exceptions = build_service_data_with_exceptions(calendar_dates_txt, trips_txt)
        shape_variant_data = build_shape_variant_data(updated_route_versions_df, df_noexceptions, df_exceptions)
        
        updated_shape_variants_df, updated_shape_variant_activations_df = update_shape_variants_and_activations(
            shape_variant_data, shape_variants_df, shape_variant_activations_df
        )
        
        # Step 6: Save data if requested
        if save_data:
            print("7. Saving processed data...")
            save_routes(updated_routes_df, self.data_folder)
            save_route_versions(updated_route_versions_df, self.data_folder)
            save_shape_variants(updated_shape_variants_df, self.data_folder)
            save_shape_variant_activations(updated_shape_variant_activations_df, self.data_folder)
            print("Processing completed successfully!")
        
        # Return all processed data
        return {
            'shapes': shapes_df,
            'routes': updated_routes_df,
            'route_versions': updated_route_versions_df,
            'shape_variants': updated_shape_variants_df,
            'shape_variant_activations': updated_shape_variant_activations_df,
            'temporary_changes': temporary_changes_df,
            'latest_routes': latest_routes_df,
            'shape_variant_data': shape_variant_data
        }


def process_transit_data(date: str, data_folder: Optional[str] = None, save_data: bool = True) -> dict:
    """
    Convenience function to process transit data for a specific date.
    
    Args:
        date: Date string (e.g., '20131018')
        data_folder: Custom data folder path. If None, uses default.
        save_data: Whether to save processed data to files
        
    Returns:
        Dictionary containing all processed DataFrames
    """
    processor = TransitDataProcessor(data_folder)
    return processor.process_date(date, save_data)
