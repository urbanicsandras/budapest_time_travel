"""
Shape data management functions for transit data processing.
"""
import pandas as pd
from typing import Set, List


def update_shapes_from_variants(shapes_df: pd.DataFrame, shape_variant_data: pd.DataFrame, 
                               shapes_txt: pd.DataFrame) -> pd.DataFrame:
    """
    Update shapes_df with any missing shape_ids from shape_variant_data.
    
    Args:
        shapes_df: Existing shapes DataFrame
        shape_variant_data: DataFrame containing shape variant data with shape_id column
        shapes_txt: Source shapes data from GTFS
        
    Returns:
        Updated shapes DataFrame with new shapes added
    """
    # Get all shape_ids from variant data
    variant_shape_ids = set(shape_variant_data['shape_id'].unique())
    
    # Get existing shape_ids in shapes_df
    if shapes_df.empty:
        existing_shape_ids = set()
    else:
        existing_shape_ids = set(shapes_df['shape_id'].unique())
    
    # Find missing shape_ids
    missing_shape_ids = variant_shape_ids - existing_shape_ids
    
    if not missing_shape_ids:
        print("All shape_ids from shape variants already exist in shapes_df.")
        return shapes_df
    
    print(f"Found {len(missing_shape_ids)} missing shape_ids in shapes_df.")
    print(f"Missing shape_ids: {sorted(list(missing_shape_ids))}")
    
    # Get missing shapes from shapes_txt
    missing_shapes = shapes_txt[shapes_txt['shape_id'].isin(missing_shape_ids)].copy()
    
    if missing_shapes.empty:
        print("Warning: Missing shape_ids not found in shapes_txt!")
        return shapes_df
    
    # Check if any shape_ids are still missing
    found_shape_ids = set(missing_shapes['shape_id'].unique())
    still_missing = missing_shape_ids - found_shape_ids
    
    if still_missing:
        print(f"Warning: {len(still_missing)} shape_ids not found in shapes_txt: {sorted(list(still_missing))}")
    
    # Add new shapes to shapes_df
    if not missing_shapes.empty:
        updated_shapes_df = pd.concat([shapes_df, missing_shapes], ignore_index=True)
        
        # Sort by shape_id and shape_pt_sequence for consistency
        updated_shapes_df = updated_shapes_df.sort_values(['shape_id', 'shape_pt_sequence']).reset_index(drop=True)
        
        print(f"Added {len(missing_shapes)} shape records to shapes_df.")
        print(f"New shapes_df shape: {updated_shapes_df.shape}")
        
        return updated_shapes_df
    
    return shapes_df


def validate_shape_integrity(shapes_df: pd.DataFrame, shape_variant_data: pd.DataFrame) -> dict:
    """
    Validate that all shape_ids in shape_variant_data exist in shapes_df.
    
    Args:
        shapes_df: Shapes DataFrame
        shape_variant_data: Shape variant data DataFrame
        
    Returns:
        Dictionary with validation results
    """
    variant_shape_ids = set(shape_variant_data['shape_id'].unique())
    
    if shapes_df.empty:
        existing_shape_ids = set()
    else:
        existing_shape_ids = set(shapes_df['shape_id'].unique())
    
    missing_shape_ids = variant_shape_ids - existing_shape_ids
    
    validation_result = {
        'is_valid': len(missing_shape_ids) == 0,
        'total_variant_shapes': len(variant_shape_ids),
        'existing_shapes': len(existing_shape_ids),
        'missing_shape_ids': sorted(list(missing_shape_ids)),
        'missing_count': len(missing_shape_ids)
    }
    
    return validation_result


def get_shape_statistics(shapes_df: pd.DataFrame) -> dict:
    """
    Get statistics about the shapes DataFrame.
    
    Args:
        shapes_df: Shapes DataFrame
        
    Returns:
        Dictionary with shape statistics
    """
    if shapes_df.empty:
        return {
            'total_records': 0,
            'unique_shapes': 0,
            'avg_points_per_shape': 0,
            'min_points_per_shape': 0,
            'max_points_per_shape': 0
        }
    
    # Group by shape_id to get points per shape
    shape_counts = shapes_df.groupby('shape_id').size()
    
    stats = {
        'total_records': len(shapes_df),
        'unique_shapes': len(shape_counts),
        'avg_points_per_shape': round(shape_counts.mean(), 2),
        'min_points_per_shape': shape_counts.min(),
        'max_points_per_shape': shape_counts.max()
    }
    
    return stats


def print_shape_summary(shapes_df: pd.DataFrame, operation: str = "Current") -> None:
    """
    Print a summary of the shapes DataFrame.
    
    Args:
        shapes_df: Shapes DataFrame
        operation: Description of the operation (e.g., "Before update", "After update")
    """
    stats = get_shape_statistics(shapes_df)
    
    print(f"\n=== {operation} Shapes Summary ===")
    print(f"Total shape records: {stats['total_records']:,}")
    print(f"Unique shapes: {stats['unique_shapes']:,}")
    if stats['unique_shapes'] > 0:
        print(f"Average points per shape: {stats['avg_points_per_shape']}")
        print(f"Points per shape range: {stats['min_points_per_shape']} - {stats['max_points_per_shape']}")
