"""
Transit Data Processing Package

A modular package for processing GTFS transit data and managing route versions,
shape variants, and schedule activations.
"""

from .pipeline import TransitDataProcessor, process_transit_data
from .flexible_date_processor import (
    FlexibleDateProcessor, 
    process_single_date, 
    process_date_range, 
    process_date_list
)
from .data_loader import load_gtfs_data, load_processed_data
from .date_utils import get_active_dates, build_service_date_mappings
from .route_processor import build_latest_routes, update_routes, update_route_versions
from .shape_processor import (
    build_service_data_without_exceptions,
    build_service_data_with_exceptions,
    build_shape_variant_data,
    update_shape_variants_and_activations
)
from .processing_tracker import ProcessingTracker
from .shapes_updater import update_shapes_from_variants, validate_shape_integrity, print_shape_summary
from .data_saver import (
    save_routes, save_route_versions, save_shape_variants, 
    save_shape_variant_activations, save_all_processed_data, save_shapes
)
from .config import Config, PathManager

__version__ = "1.1.0"
__author__ = "Your Name"

# Main exports for easy access
__all__ = [
    # Main processing classes
    'TransitDataProcessor',
    'FlexibleDateProcessor',
    'ProcessingTracker',
    
    # High-level processing functions
    'process_transit_data',
    'process_single_date',
    'process_date_range', 
    'process_date_list',
    
    # Data loading
    'load_gtfs_data',
    'load_processed_data',
    
    # Date utilities
    'get_active_dates',
    'build_service_date_mappings',
    
    # Route processing
    'build_latest_routes',
    'update_routes',
    'update_route_versions',
    
    # Shape processing
    'build_service_data_without_exceptions',
    'build_service_data_with_exceptions',
    'build_shape_variant_data',
    'update_shape_variants_and_activations',
    
    # Shape data management
    'update_shapes_from_variants',
    'validate_shape_integrity',
    'print_shape_summary',
    
    # Data saving
    'save_routes',
    'save_route_versions',
    'save_shape_variants',
    'save_shape_variant_activations',
    'save_all_processed_data',
    'save_shapes',
    
    # Configuration
    'Config',
    'PathManager',
]