"""
Data saving functions for processed transit data.
"""
import pandas as pd
from typing import Optional

from .config import PathManager, Config


def save_routes(routes_df: pd.DataFrame, data_folder: Optional[str] = None, show_progress: bool = True) -> None:
    """
    Save routes DataFrame to CSV file.
    
    Args:
        routes_df: Routes DataFrame to save
        data_folder: Custom data folder path. If None, uses auto-detected path.
        show_progress: Whether to show progress messages
    """
    if data_folder is None:
        data_folder = Config.get_default_processed_data_folder()
        
    file_paths = PathManager.get_processed_data_paths(data_folder)
    routes_path = file_paths[1]  # routes.csv is at index 1
    
    routes_df.to_csv(routes_path, index=False)
    if show_progress:
        print(f"routes_df saved to {routes_path}")


def save_route_versions(route_versions_df: pd.DataFrame, data_folder: Optional[str] = None, show_progress: bool = True) -> None:
    """
    Save route versions DataFrame to CSV file.
    
    Args:
        route_versions_df: Route versions DataFrame to save
        data_folder: Custom data folder path. If None, uses auto-detected path.
        show_progress: Whether to show progress messages
    """
    if data_folder is None:
        data_folder = Config.get_default_processed_data_folder()
        
    file_paths = PathManager.get_processed_data_paths(data_folder)
    route_versions_path = file_paths[2]  # route_versions.csv is at index 2
    
    route_versions_df.to_csv(route_versions_path, index=False)
    if show_progress:
        print(f"route_versions_df saved to {route_versions_path}")


def save_shape_variants(shape_variants_df: pd.DataFrame, data_folder: Optional[str] = None, show_progress: bool = True) -> None:
    """
    Save shape variants DataFrame to CSV file.
    
    Args:
        shape_variants_df: Shape variants DataFrame to save
        data_folder: Custom data folder path. If None, uses auto-detected path.
        show_progress: Whether to show progress messages
    """
    if data_folder is None:
        data_folder = Config.get_default_processed_data_folder()
        
    file_paths = PathManager.get_processed_data_paths(data_folder)
    shape_variants_path = file_paths[3]  # shape_variants.csv is at index 3
    
    shape_variants_df.to_csv(shape_variants_path, index=False)
    if show_progress:
        print(f"shape_variants_df saved to {shape_variants_path}")


def save_shape_variant_activations(shape_variant_activations_df: pd.DataFrame, 
                                  data_folder: Optional[str] = None, show_progress: bool = True) -> None:
    """
    Save shape variant activations DataFrame to CSV file.
    
    Args:
        shape_variant_activations_df: Shape variant activations DataFrame to save
        data_folder: Custom data folder path. If None, uses auto-detected path.
        show_progress: Whether to show progress messages
    """
    if data_folder is None:
        data_folder = Config.get_default_processed_data_folder()
        
    file_paths = PathManager.get_processed_data_paths(data_folder)
    activations_path = file_paths[4]  # shape_variant_activations.csv is at index 4
    
    shape_variant_activations_df.to_csv(activations_path, index=False)
    if show_progress:
        print(f"shape_variant_activations_df saved to {activations_path}")


def save_shapes(shapes_df: pd.DataFrame, data_folder: Optional[str] = None, show_progress: bool = True) -> None:
    """
    Save shapes DataFrame to CSV file.
    
    Args:
        shapes_df: Shapes DataFrame to save
        data_folder: Custom data folder path. If None, uses auto-detected path.
        show_progress: Whether to show progress messages
    """
    if data_folder is None:
        data_folder = Config.get_default_processed_data_folder()
        
    file_paths = PathManager.get_processed_data_paths(data_folder)
    shapes_path = file_paths[0]  # shapes.csv is at index 0
    
    shapes_df.to_csv(shapes_path, index=False)
    if show_progress:
        print(f"shapes_df saved to {shapes_path}")


def save_temporary_changes(temporary_changes_df: pd.DataFrame, 
                          data_folder: Optional[str] = None, show_progress: bool = True) -> None:
    """
    Save temporary changes DataFrame to CSV file.
    
    Args:
        temporary_changes_df: Temporary changes DataFrame to save
        data_folder: Custom data folder path. If None, uses auto-detected path.
        show_progress: Whether to show progress messages
    """
    if data_folder is None:
        data_folder = Config.get_default_processed_data_folder()
        
    file_paths = PathManager.get_processed_data_paths(data_folder)
    temp_changes_path = file_paths[5]  # temporary_changes.csv is at index 5
    
    temporary_changes_df.to_csv(temp_changes_path, index=False)
    if show_progress:
        print(f"temporary_changes_df saved to {temp_changes_path}")


def save_all_processed_data(shapes_df: pd.DataFrame, routes_df: pd.DataFrame,
                           route_versions_df: pd.DataFrame, shape_variants_df: pd.DataFrame,
                           shape_variant_activations_df: pd.DataFrame, 
                           temporary_changes_df: pd.DataFrame,
                           data_folder: Optional[str] = None, show_progress: bool = True) -> None:
    """
    Save all processed DataFrames to CSV files.
    
    Args:
        shapes_df: Shapes DataFrame
        routes_df: Routes DataFrame
        route_versions_df: Route versions DataFrame
        shape_variants_df: Shape variants DataFrame
        shape_variant_activations_df: Shape variant activations DataFrame
        temporary_changes_df: Temporary changes DataFrame
        data_folder: Custom data folder path. If None, uses auto-detected path.
        show_progress: Whether to show progress messages
    """
    save_shapes(shapes_df, data_folder, show_progress)
    save_routes(routes_df, data_folder, show_progress)
    save_route_versions(route_versions_df, data_folder, show_progress)
    save_shape_variants(shape_variants_df, data_folder, show_progress)
    save_shape_variant_activations(shape_variant_activations_df, data_folder, show_progress)
    save_temporary_changes(temporary_changes_df, data_folder, show_progress)
    if show_progress:
        print("All processed data saved successfully!")