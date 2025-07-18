"""
Updated processing pipeline with proper progress control.
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
from .shapes_updater import update_shapes_from_variants, validate_shape_integrity, print_shape_summary
from .data_saver import save_routes, save_route_versions, save_shape_variants, save_shape_variant_activations, save_shapes


class TransitDataProcessor:
    """Main class for processing transit data."""
    
    def __init__(self, data_folder: Optional[str] = None, raw_data_folder: Optional[str] = None):
        """
        Initialize the processor.
        
        Args:
            data_folder: Custom processed data folder path. If None, uses auto-detected path.
            raw_data_folder: Custom raw data folder path. If None, uses auto-detected path.
        """
        self.data_folder = data_folder
        self.raw_data_folder = raw_data_folder
        
    def process_date(self, date: str, save_data: bool = True, return_data: bool = False, 
                    show_progress: bool = True) -> dict:
        """
        Process transit data for a specific date.
        
        Args:
            date: Date string (e.g., '20131018')
            save_data: Whether to save processed data to files
            return_data: Whether to return the processed DataFrames dictionary
            show_progress: Whether to show internal processing steps
            
        Returns:
            Dictionary containing all processed DataFrames if return_data=True, 
            empty dict otherwise
        """
        if show_progress:
            print(f"Processing transit data for date: {date}")
        
        # Step 1: Load data
        if show_progress:
            print("1. Loading GTFS data...")
        routes_txt, trips_txt, shapes_txt, calendar_txt, calendar_dates_txt = load_gtfs_data(date, self.raw_data_folder)
        
        if show_progress:
            print("2. Loading existing processed data...")
        (shapes_df, routes_df, route_versions_df, shape_variants_df, 
         shape_variant_activations_df, temporary_changes_df) = load_processed_data(self.data_folder)
        
        # Step 2: Build service date mappings
        if show_progress:
            print("3. Building service date mappings...")
        trip_dates, trip_first_date = build_service_date_mappings(trips_txt, calendar_txt)
        
        # Step 3: Process routes
        if show_progress:
            print("4. Processing routes...")
        latest_routes_df = build_latest_routes(trips_txt, trip_first_date, routes_txt)
        updated_routes_df = update_routes(routes_df, latest_routes_df, show_progress)
        
        # Step 4: Process route versions (pass show_progress parameter)
        if show_progress:
            print("5. Processing route versions...")
        updated_route_versions_df = update_route_versions(route_versions_df, latest_routes_df, date, show_progress)
        
        # Step 6: Process shape variants
        if show_progress:
            print("6. Processing shape variants...")
        df_noexceptions = build_service_data_without_exceptions(trip_dates, trips_txt)
        df_exceptions = build_service_data_with_exceptions(calendar_dates_txt, trips_txt)
        shape_variant_data = build_shape_variant_data(updated_route_versions_df, df_noexceptions, df_exceptions, show_progress)
        
        updated_shape_variants_df, updated_shape_variant_activations_df = update_shape_variants_and_activations(
            shape_variant_data, shape_variants_df, shape_variant_activations_df, show_progress
        )
        
        # Step 7: Update shapes_df with any missing shapes
        if show_progress:
            print("7. Updating shapes data...")
            print_shape_summary(shapes_df, "Before update")
        
        # Validate current shape integrity
        validation = validate_shape_integrity(shapes_df, shape_variant_data)
        if not validation['is_valid'] and show_progress:
            print(f"Found {validation['missing_count']} missing shape_ids that need to be added.")
        
        # Update shapes_df with missing shapes
        updated_shapes_df = update_shapes_from_variants(shapes_df, shape_variant_data, shapes_txt, show_progress)
        if show_progress:
            print_shape_summary(updated_shapes_df, "After update")
        
        # Step 8: Save data if requested
        if save_data:
            if show_progress:
                print("8. Saving processed data...")
            save_shapes(updated_shapes_df, self.data_folder, show_progress)
            save_routes(updated_routes_df, self.data_folder, show_progress)
            save_route_versions(updated_route_versions_df, self.data_folder, show_progress)
            save_shape_variants(updated_shape_variants_df, self.data_folder, show_progress)
            save_shape_variant_activations(updated_shape_variant_activations_df, self.data_folder, show_progress)
            if show_progress:
                print("Processing completed successfully!")
        
        # Return all processed data if requested
        if return_data:
            return {
                'shapes': updated_shapes_df,
                'routes': updated_routes_df,
                'route_versions': updated_route_versions_df,
                'shape_variants': updated_shape_variants_df,
                'shape_variant_activations': updated_shape_variant_activations_df,
                'temporary_changes': temporary_changes_df,
                'latest_routes': latest_routes_df,
                'shape_variant_data': shape_variant_data
            }
        else:
            return {}


def process_transit_data(date: str, data_folder: Optional[str] = None, raw_data_folder: Optional[str] = None,
                        save_data: bool = True, return_data: bool = False, show_progress: bool = True) -> dict:
    """
    Convenience function to process transit data for a specific date.
    
    Args:
        date: Date string (e.g., '20131018')
        data_folder: Custom processed data folder path. If None, uses auto-detected path.
        raw_data_folder: Custom raw data folder path. If None, uses auto-detected path.
        save_data: Whether to save processed data to files
        return_data: Whether to return the processed DataFrames dictionary
        show_progress: Whether to show internal processing steps
        
    Returns:
        Dictionary containing all processed DataFrames if return_data=True, 
        empty dict otherwise
    """
    processor = TransitDataProcessor(data_folder, raw_data_folder)
    return processor.process_date(date, save_data, return_data, show_progress)