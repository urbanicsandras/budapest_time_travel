"""
Shape variant processing functions for transit data.
"""
import pandas as pd
import numpy as np
from typing import Dict, List

from .config import Config


def build_service_data_without_exceptions(trip_dates: Dict[str, List[str]], 
                                        trips_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build service data without calendar exceptions.
    
    Args:
        trip_dates: Dictionary mapping service IDs to their active dates
        trips_df: DataFrame with trip data
        
    Returns:
        DataFrame with service data without exceptions
    """
    non_empty_keys = [key for key, value in trip_dates.items() if value]

    inservice_df = trips_df[trips_df["service_id"].isin(non_empty_keys)]
    inservice_df = inservice_df[["service_id", "route_id", "shape_id", "trip_headsign", "direction_id"]]
    inservice_df = inservice_df.groupby(["route_id", "shape_id", "trip_headsign", "direction_id"]).agg("first").reset_index()

    # Add the list column and explode
    inservice_df['date_list'] = inservice_df['service_id'].map(trip_dates)
    df_noexceptions = inservice_df.explode('date_list')
    df_noexceptions = df_noexceptions.rename(columns={'date_list': 'date'})
    df_noexceptions.drop(columns=['service_id'], inplace=True)
    df_noexceptions['exception_type'] = np.nan
    
    return df_noexceptions


def build_service_data_with_exceptions(calendar_dates_df: pd.DataFrame, 
                                     trips_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build service data with calendar exceptions.
    
    Args:
        calendar_dates_df: DataFrame with calendar dates/exceptions
        trips_df: DataFrame with trip data
        
    Returns:
        DataFrame with service data including exceptions
    """
    calendar_dates_df['date'] = pd.to_datetime(calendar_dates_df['date'], format="%Y%m%d")

    extra_service_ids = calendar_dates_df[["date", "service_id", "exception_type"]].copy()
    extra_service_ids["exception_type"] = extra_service_ids["exception_type"].astype(int)

    df_exceptions = pd.merge(trips_df, extra_service_ids, how="left", on="service_id")
    df_exceptions = df_exceptions.groupby(["route_id", "shape_id", "trip_headsign", "direction_id", "date"]).agg('first').reset_index()
    df_exceptions = df_exceptions[["date", "route_id", "shape_id", "trip_headsign", "direction_id", "exception_type"]]

    # Convert exception_type to string, keeping NaN as NaN
    df_exceptions["exception_type"] = df_exceptions["exception_type"].apply(
        lambda x: str(int(x)) if pd.notna(x) else x
    )

    return df_exceptions


def merge_service_data(df_noexceptions: pd.DataFrame, df_exceptions: pd.DataFrame) -> pd.DataFrame:
    """
    Merge service data with and without exceptions, handling duplicates.
    
    Args:
        df_noexceptions: DataFrame with service data without exceptions
        df_exceptions: DataFrame with service data with exceptions
        
    Returns:
        Merged DataFrame with duplicates removed
    """
    df1 = df_noexceptions.copy()
    df2 = df_exceptions.copy()

    # Ensure consistent date format
    df1['date'] = pd.to_datetime(df1['date']).dt.strftime('%Y-%m-%d')
    df2['date'] = pd.to_datetime(df2['date']).dt.strftime('%Y-%m-%d')

    # Concatenate the dataframes
    combined = pd.concat([df1, df2], ignore_index=True)

    # Columns for duplicate checking (excluding exception_type)
    cols_except_exception = [col for col in combined.columns if col != 'exception_type']

    # Mark rows where exception_type is NaN
    combined['__is_nan__'] = combined['exception_type'].isna()

    # Sort: non-NaN first
    combined_sorted = combined.sort_values('__is_nan__')

    # Remove duplicates, keeping first (non-NaN exception_type preferred)
    before = len(combined_sorted)
    merged_df = combined_sorted.drop_duplicates(subset=cols_except_exception, keep='first')
    after = len(merged_df)
    removed = before - after

    # Remove helper column
    merged_df = merged_df.drop(columns='__is_nan__')

    # Sort by specified columns
    sort_columns = ['date', 'route_id', 'direction_id', 'shape_id', 'trip_headsign', 'exception_type']
    merged_df = merged_df.sort_values(by=sort_columns)

    # Reset index
    merged_df = merged_df.reset_index(drop=True)

    print(f"Removed {removed} duplicate rows where only exception_type differed (NaN vs non-NaN).")
    return merged_df


def build_shape_variant_data(route_versions_df: pd.DataFrame, df_noexceptions: pd.DataFrame, 
                           df_exceptions: pd.DataFrame) -> pd.DataFrame:
    """
    Build shape variant data by merging route versions with service data.
    
    Args:
        route_versions_df: DataFrame with route versions
        df_noexceptions: DataFrame with service data without exceptions
        df_exceptions: DataFrame with service data with exceptions
        
    Returns:
        DataFrame with shape variant data
    """
    valid_routes = route_versions_df[route_versions_df["valid_to"].isna()][["version_id", "route_id", "direction_id", "main_shape_id"]]

    merged_df = merge_service_data(df_noexceptions, df_exceptions)
    return_df = pd.merge(valid_routes, merged_df, on=["route_id", "direction_id"])
    return_df["main_shape_id"] = (return_df["main_shape_id"] == return_df["shape_id"]).astype(int)
    return_df = return_df.rename(columns={"main_shape_id": "is_main"})
    print("return_df: ", return_df.columns)
    return return_df


def update_shape_variants_and_activations(shape_variant_data: pd.DataFrame, 
                                         shape_variants_df: pd.DataFrame,
                                         shape_variant_activations_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Update shape variants and activations DataFrames with new data.
    
    Args:
        shape_variant_data: DataFrame with new shape variant data
        shape_variants_df: Existing shape variants DataFrame
        shape_variant_activations_df: Existing shape variant activations DataFrame
        
    Returns:
        Tuple of updated (shape_variants_df, shape_variant_activations_df)
    """
    # Get unique shape variants from merged_df
    new_variants = shape_variant_data[['version_id', 'shape_id', 'trip_headsign', 'is_main']].drop_duplicates().reset_index(drop=True)

    # Check which variants are already in shape_variants_df
    if not shape_variants_df.empty:
        existing_variants = shape_variants_df[['version_id', 'shape_id', 'trip_headsign', 'is_main']]
        # Find variants that don't already exist
        merged_check = new_variants.merge(
            existing_variants, 
            on=['version_id', 'shape_id', 'trip_headsign', 'is_main'], 
            how='left', 
            indicator=True
        )
        truly_new_variants = merged_check[merged_check['_merge'] == 'left_only'].drop('_merge', axis=1).reset_index(drop=True)
    else:
        truly_new_variants = new_variants

    # Add new variants to shape_variants_df
    if not truly_new_variants.empty:
        # Determine starting shape_variant_id
        if shape_variants_df.empty:
            start_id = Config.START_SHAPE_VARIANT_ID
        else:
            start_id = shape_variants_df['shape_variant_id'].max() + 1
        
        # Create new variant records
        new_variant_records = truly_new_variants.copy()
        new_variant_records['shape_variant_id'] = range(start_id, start_id + len(truly_new_variants))
        new_variant_records['note'] = None
        new_variant_records = new_variant_records[['shape_variant_id', 'version_id', 'shape_id', 'trip_headsign', 'is_main', 'note']]
        
        # Append to existing shape_variants_df
        shape_variants_df = pd.concat([shape_variants_df, new_variant_records], ignore_index=True)

    # Create mapping for all variants (existing + new)
    variant_mapping = shape_variants_df[['shape_variant_id', 'version_id', 'shape_id', 'trip_headsign', 'is_main']].copy()

    # Merge with variant mapping to get shape_variant_id for each row
    merged_with_variant_id = shape_variant_data.merge(
        variant_mapping, 
        on=['version_id', 'shape_id', 'trip_headsign', 'is_main'], 
        how='left'
    )

    # Create new activation records
    new_activations = merged_with_variant_id[['date', 'shape_variant_id', 'exception_type']].copy()
    new_activations['exception_type'] = new_activations['exception_type'].astype('float64')

    # Check which activations are already in shape_variant_activations_df
    if not shape_variant_activations_df.empty:
        # Find activations that don't already exist
        merged_activations_check = new_activations.merge(
            shape_variant_activations_df, 
            on=['date', 'shape_variant_id', 'exception_type'], 
            how='left', 
            indicator=True
        )
        truly_new_activations = merged_activations_check[merged_activations_check['_merge'] == 'left_only'].drop('_merge', axis=1).reset_index(drop=True)
    else:
        truly_new_activations = new_activations

    # Add new activations to shape_variant_activations_df
    if not truly_new_activations.empty:
        shape_variant_activations_df = pd.concat([shape_variant_activations_df, truly_new_activations], ignore_index=True)

    shape_variant_activations_df.sort_values(['date', 'shape_variant_id'], inplace=True)
    shape_variant_activations_df.reset_index(drop=True, inplace=True)

    # Display results
    print("Updated shape_variants_df:")
    print(f"Shape: {shape_variants_df.shape}")
    print()

    print("Updated shape_variant_activations_df:")
    print(f"Shape: {shape_variant_activations_df.shape}")

    # Summary
    print(f"\nSummary:")
    print(f"Total unique shape variants: {len(shape_variants_df)}")
    print(f"Total shape variant activations: {len(shape_variant_activations_df)}")
    if not truly_new_variants.empty:
        print(f"New variants added: {len(truly_new_variants)}")
        if 'new_variant_records' in locals():
            print(f"Shape variant IDs added: {new_variant_records['shape_variant_id'].min()} - {new_variant_records['shape_variant_id'].max()}")
    else:
        print("No new variants added")
    if not truly_new_activations.empty:
        print(f"New activations added: {len(truly_new_activations)}")
    else:
        print("No new activations added")

    return shape_variants_df, shape_variant_activations_df