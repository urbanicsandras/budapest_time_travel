"""
Route processing functions for transit data.
"""
import pandas as pd
import numpy as np
from typing import Dict, Optional

from .config import Config


def build_latest_routes(trips_df: pd.DataFrame, trip_first_date: Dict[str, Optional[str]], 
                       routes_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build latest routes DataFrame from trips data.
    
    Args:
        trips_df: DataFrame with trip data
        trip_first_date: Dictionary mapping service IDs to their first dates
        routes_df: DataFrame with route data
        
    Returns:
        DataFrame with latest route information
    """
    extended_trips = trips_df.copy()
    extended_trips["first_date"] = extended_trips["service_id"].map(trip_first_date)
    
    # Group and aggregate
    extended_trips = extended_trips[["service_id", "route_id", "shape_id", "trip_headsign", "direction_id", "first_date"]]
    extended_trips = extended_trips.groupby(["route_id", "shape_id", "trip_headsign", "direction_id", "first_date"]).count().reset_index()
    extended_trips = extended_trips.sort_values(by=['route_id', 'direction_id', 'service_id'], ascending=[True, True, False])
    extended_trips = extended_trips.drop_duplicates(subset=['route_id', 'direction_id'], ignore_index=True)
    extended_trips = extended_trips.rename(columns={"shape_id": "main_shape_id", "first_date": "valid_from"})
    
    # Merge with routes data
    latest_routes_df = pd.merge(
        routes_df,
        extended_trips[["route_id", "main_shape_id", "trip_headsign", "direction_id", "valid_from"]],
        on="route_id",
        how="inner"
    )
    
    return latest_routes_df


def update_routes(routes_df: pd.DataFrame, latest_routes_df: pd.DataFrame, show_progress: bool = True) -> pd.DataFrame:
    """
    Update routes DataFrame with new routes.
    
    Args:
        routes_df: Existing routes DataFrame
        latest_routes_df: Latest routes DataFrame
        show_progress: Whether to show progress messages
        
    Returns:
        Updated routes DataFrame
    """
    # Use relevant columns
    cols_to_use = [col for col in routes_df.columns]

    # Select new rows - routes not in existing routes_df 
    new_routes = latest_routes_df[~latest_routes_df["route_id"].isin(routes_df["route_id"])][cols_to_use]
    
    # Concatenate new routes
    updated_routes_df = pd.concat([routes_df, new_routes], ignore_index=True)

    # Check for duplicates
    duplicates = updated_routes_df[updated_routes_df.groupby("route_id")["route_id"].transform("count") > 2]

    if show_progress:
        if not duplicates.empty:
            print(f"Warning: There are {duplicates['route_id'].nunique()} duplicated route_id(s) in routes_df!")
            print("Duplicated route_id(s):")
            print(duplicates['route_id'].unique())
        else:
            print("No duplicate route_id found in routes_df.")

    return updated_routes_df


def version_exists(current_versions: pd.DataFrame, row: pd.Series) -> bool:
    """
    Check if a route version already exists.
    
    Args:
        current_versions: DataFrame with current route versions
        row: Series representing a route version to check
        
    Returns:
        True if version exists, False otherwise
    """
    return (
        ((current_versions["route_id"] == row["route_id"]) &
         (current_versions["direction_id"] == row["direction_id"]) &
         (current_versions["main_shape_id"] == row["main_shape_id"]) &
         (current_versions["trip_headsign"] == row["trip_headsign"]))
        .any()
    )


def update_route_versions(route_versions_df: pd.DataFrame, latest_routes_df: pd.DataFrame, 
                         date: str) -> pd.DataFrame:
    """
    Update route versions DataFrame with new versions.
    
    Args:
        route_versions_df: Existing route versions DataFrame
        latest_routes_df: Latest routes DataFrame
        date: Processing date
        
    Returns:
        Updated route versions DataFrame
    """
    route_versions_copy_df = route_versions_df.copy()
    
    # Determine next version ID
    if route_versions_df.empty:
        next_version_id = Config.START_VERSION_ID
    else:
        next_version_id = route_versions_df["version_id"].max() + 1

    # Create new versions dataframe
    new_versions_df = latest_routes_df.copy()[["route_id", "main_shape_id", "trip_headsign", "direction_id", "route_desc", "valid_from"]]
    new_versions_df["valid_from"] = pd.to_datetime(new_versions_df['valid_from'])
    new_versions_df["valid_to"] = pd.NaT
    new_versions_df["parent_version_id"] = np.nan
    new_versions_df["note"] = np.nan

    # Define current versions (those without valid_to date)
    current_versions = route_versions_df[route_versions_df["valid_to"].isna()]

    # Filter for truly new versions
    new_versions_filtered = new_versions_df[~new_versions_df.apply(lambda row: version_exists(current_versions, row), axis=1)].copy()

    # Update previous versions' valid_to date
    for _, row in new_versions_filtered.iterrows():
        mask = (
            (route_versions_df["route_id"] == row["route_id"]) &
            (route_versions_df["valid_to"].isna())
        )
        route_versions_copy_df.loc[mask, "valid_to"] = row["valid_from"] - pd.Timedelta(days=1)

    # Assign version IDs to new versions
    new_versions_filtered["version_id"] = range(next_version_id, next_version_id + len(new_versions_filtered))

    # Concatenate with existing versions
    extended_route_versions_df = pd.concat([route_versions_copy_df, new_versions_filtered], ignore_index=True)

    return extended_route_versions_df