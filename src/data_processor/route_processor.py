"""
Fixed route processing functions for transit data with proper version management.
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
    
    # Concatenate new routes (handle empty DataFrames properly)
    if new_routes.empty:
        updated_routes_df = routes_df.copy()
    elif routes_df.empty:
        updated_routes_df = new_routes.copy()
    else:
        updated_routes_df = pd.concat([routes_df, new_routes], ignore_index=True)

    # Check for duplicates
    duplicates = updated_routes_df[updated_routes_df.groupby("route_id")["route_id"].transform("count") > 1]

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
                         date: str, show_progress: bool = True) -> pd.DataFrame:
    """
    Update route versions DataFrame with new versions, properly handling overlaps and duplicates.
    
    Args:
        route_versions_df: Existing route versions DataFrame
        latest_routes_df: Latest routes DataFrame
        date: Processing date
        show_progress: Whether to show detailed progress messages
        
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

    if new_versions_filtered.empty:
        # No new versions to add
        return route_versions_copy_df

    # FIXED LOGIC: Update previous versions' valid_to date CORRECTLY
    for _, new_row in new_versions_filtered.iterrows():
        # Find all current (active) versions for this route and direction
        active_versions_mask = (
            (route_versions_copy_df["route_id"] == new_row["route_id"]) &
            (route_versions_copy_df["direction_id"] == new_row["direction_id"]) &
            (route_versions_copy_df["valid_to"].isna())
        )
        
        # Get the indices of active versions for this route/direction
        active_indices = route_versions_copy_df[active_versions_mask].index
        
        if len(active_indices) > 0:
            # Set valid_to to the day before the new version starts
            new_valid_to = new_row["valid_from"] - pd.Timedelta(days=1)
            
            # Update all active versions for this route/direction
            route_versions_copy_df.loc[active_indices, "valid_to"] = new_valid_to
            
            if show_progress:
                print(f"Updated {len(active_indices)} existing version(s) for route {new_row['route_id']} direction {new_row['direction_id']}")
                print(f"  Set valid_to to: {new_valid_to.strftime('%Y-%m-%d')}")

    # Assign version IDs to new versions
    new_versions_filtered["version_id"] = range(next_version_id, next_version_id + len(new_versions_filtered))

    # Concatenate with existing versions (handle empty DataFrames properly)
    if new_versions_filtered.empty:
        extended_route_versions_df = route_versions_copy_df
    elif route_versions_copy_df.empty:
        extended_route_versions_df = new_versions_filtered
    else:
        extended_route_versions_df = pd.concat([route_versions_copy_df, new_versions_filtered], ignore_index=True)

    # ADDITIONAL VALIDATION: Check for overlaps and fix them
    #####extended_route_versions_df = fix_version_overlaps(extended_route_versions_df, show_progress)

    return extended_route_versions_df


def fix_version_overlaps(route_versions_df: pd.DataFrame, show_progress: bool = True) -> pd.DataFrame:
    """
    Fix any remaining overlaps in route versions by ensuring proper date sequencing.
    
    Args:
        route_versions_df: Route versions DataFrame that may have overlaps
        show_progress: Whether to show detailed progress messages
        
    Returns:
        DataFrame with overlaps fixed
    """
    df = route_versions_df.copy()
    
    # Group by route_id and direction_id to fix overlaps within each group
    groups = df.groupby(['route_id', 'direction_id'])
    
    fixed_dfs = []
    overlap_count = 0
    
    for (route_id, direction_id), group in groups:
        group_copy = group.copy().sort_values('valid_from')
        
        # Check for overlaps in this group
        for i in range(len(group_copy) - 1):
            current_row = group_copy.iloc[i]
            next_row = group_copy.iloc[i + 1]
            
            # If current version has no end date, set it to day before next version starts
            if pd.isna(current_row['valid_to']):
                new_valid_to = next_row['valid_from'] - pd.Timedelta(days=1)
                group_copy.iloc[i, group_copy.columns.get_loc('valid_to')] = new_valid_to
                overlap_count += 1
            
            # If current version ends after next version starts, fix the overlap
            elif current_row['valid_to'] >= next_row['valid_from']:
                new_valid_to = next_row['valid_from'] - pd.Timedelta(days=1)
                group_copy.iloc[i, group_copy.columns.get_loc('valid_to')] = new_valid_to
                overlap_count += 1
        
        fixed_dfs.append(group_copy)
    
    result_df = pd.concat(fixed_dfs, ignore_index=True)
    
    if overlap_count > 0 and show_progress:
        print(f"Fixed {overlap_count} date overlap(s) in route versions")
    
    return result_df


def validate_route_versions(route_versions_df: pd.DataFrame, show_details: bool = True) -> dict:
    """
    Validate route versions for common issues like overlaps and invalid date ranges.
    
    Args:
        route_versions_df: Route versions DataFrame to validate
        show_details: Whether to show detailed information about issues
        
    Returns:
        Dictionary with validation results
    """
    issues = {
        'invalid_date_ranges': [],
        'overlapping_versions': [],
        'duplicate_active_versions': [],
        'is_valid': True
    }
    
    if route_versions_df.empty:
        return issues
    
    # Check for invalid date ranges (valid_to before valid_from)
    invalid_ranges = route_versions_df[
        (route_versions_df['valid_to'].notna()) & 
        (route_versions_df['valid_to'] < route_versions_df['valid_from'])
    ]
    
    if not invalid_ranges.empty:
        issues['invalid_date_ranges'] = invalid_ranges[['version_id', 'route_id', 'direction_id', 'valid_from', 'valid_to']].to_dict('records')
        issues['is_valid'] = False
        if show_details:
            print(f"❌ Found {len(invalid_ranges)} version(s) with invalid date ranges:")
            for _, row in invalid_ranges.iterrows():
                print(f"  Version {row['version_id']}: Route {row['route_id']} Dir {row['direction_id']} - {row['valid_from']} to {row['valid_to']}")
    
    # Check for overlapping versions within same route/direction
    groups = route_versions_df.groupby(['route_id', 'direction_id'])
    
    for (route_id, direction_id), group in groups:
        group_sorted = group.sort_values('valid_from')
        
        # Check for multiple active versions (no valid_to date)
        active_versions = group_sorted[group_sorted['valid_to'].isna()]
        if len(active_versions) > 1:
            issues['duplicate_active_versions'].extend(
                active_versions[['version_id', 'route_id', 'direction_id', 'valid_from']].to_dict('records')
            )
            issues['is_valid'] = False
            if show_details:
                print(f"❌ Multiple active versions for Route {route_id} Direction {direction_id}:")
                for _, row in active_versions.iterrows():
                    print(f"  Version {row['version_id']}: from {row['valid_from']}")
        
        # Check for overlapping date ranges
        for i in range(len(group_sorted) - 1):
            current = group_sorted.iloc[i]
            next_version = group_sorted.iloc[i + 1]
            
            if pd.notna(current['valid_to']) and current['valid_to'] >= next_version['valid_from']:
                overlap_info = {
                    'route_id': route_id,
                    'direction_id': direction_id,
                    'version1_id': current['version_id'],
                    'version1_range': f"{current['valid_from']} to {current['valid_to']}",
                    'version2_id': next_version['version_id'],
                    'version2_range': f"{next_version['valid_from']} to {next_version['valid_to'] if pd.notna(next_version['valid_to']) else 'ongoing'}"
                }
                issues['overlapping_versions'].append(overlap_info)
                issues['is_valid'] = False
                if show_details:
                    print(f"❌ Overlapping versions for Route {route_id} Direction {direction_id}:")
                    print(f"  Version {current['version_id']}: {overlap_info['version1_range']}")
                    print(f"  Version {next_version['version_id']}: {overlap_info['version2_range']}")
    
    if issues['is_valid'] and show_details:
        print("✅ All route versions are valid - no overlaps or invalid date ranges found.")
    
    return issues