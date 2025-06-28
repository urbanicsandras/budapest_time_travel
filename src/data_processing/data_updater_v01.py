import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta


def define_paths(data_folder=None, date=None):
    if data_folder:
        shapes_df_path = os.path.join(data_folder, 'shapes.csv')
        routes_df_path = os.path.join(data_folder, 'routes.csv')
        route_versions_df_path = os.path.join(data_folder, 'route_versions.csv')
        shape_variants_df_path = os.path.join(data_folder, 'shape_variants.csv')
        shape_variant_activations_df_path = os.path.join(data_folder, 'shape_variant_activations.csv')
        temporary_changes_df_path = os.path.join(data_folder, 'temporary_changes.csv')
        return shapes_df_path, routes_df_path, route_versions_df_path, shape_variants_df_path, shape_variant_activations_df_path, temporary_changes_df_path

    if date:
        date_folder = '../data/raw/'
        routes_path = os.path.join(date_folder, date, 'routes.txt')
        trips_path = os.path.join(date_folder, date, 'trips.txt')
        shapes_path = os.path.join(date_folder, date, 'shapes.txt')
        calendar_path = os.path.join(date_folder, date, 'calendar.txt')
        calendar_dates_path = os.path.join(date_folder, date, 'calendar_dates.txt')

        return routes_path, trips_path, shapes_path, calendar_path, calendar_dates_path

    raise ValueError("Either data_folder or date must be provided.")


def load_txt_data(date, print_shapes=False):
    routes_path, trips_path, shapes_path, calendar_path, calendar_dates_path = define_paths(date=date)
    routes_txt = pd.read_csv(routes_path)
    trips_txt = pd.read_csv(trips_path)
    shapes_txt = pd.read_csv(shapes_path)
    calendar_dates_txt = pd.read_csv(calendar_dates_path)

    if print_shapes:
        print("Routes:", routes_txt.shape)
        print("Trips:", trips_txt.shape)
        print("Shapes:", shapes_txt.shape)
        print("Calendar Dates:", calendar_dates_txt.shape)

    try:  # Check if the file exists
        calendar_txt = pd.read_csv(calendar_path, parse_dates=['start_date', 'end_date'])
    except FileNotFoundError:
        # Make empty dataframes for the first time
        print("Calendar file not found. Creating empty dataframe.")
        calendar_txt = pd.DataFrame(columns=['service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'start_date', 'end_date'])
        calendar_txt['start_date'] = pd.to_datetime(calendar_txt['start_date'])
        calendar_txt['end_date'] = pd.to_datetime(calendar_txt['end_date'])
    
    return routes_txt, trips_txt, shapes_txt, calendar_txt, calendar_dates_txt


def load_df_data(data_folder):
    shapes_df_path, routes_df_path, route_versions_df_path, shape_variants_df_path, shape_variant_activations_df_path, temporary_changes_df_path = define_paths(data_folder=data_folder)

    try:
        shapes_df = pd.read_csv(shapes_df_path)
        routes_df = pd.read_csv(routes_df_path)
        route_versions_df = pd.read_csv(route_versions_df_path, parse_dates=['valid_from', 'valid_to'])
        shape_variants_df = pd.read_csv(shape_variants_df_path)
        shape_variant_activations_df = pd.read_csv(shape_variant_activations_df_path)
        temporary_changes_df = pd.read_csv(temporary_changes_df_path)
    except FileNotFoundError:
        # Make empty dataframes for the first time
        ### shapes_df ###
        shapes_df = pd.DataFrame(columns=[
            "shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence", "shape_dist_traveled", "shape_bkk_ref"
        ])

        ### routes_df ###
        routes_df = pd.DataFrame(columns=[
            "route_id", "agency_id", "route_short_name", "route_type", "route_color", "route_text_color"
        ])

        ### route_versions_df ###
        route_versions_df = pd.DataFrame(columns=[
            "version_id", "route_id", "direction_id", "route_long_name", "route_desc",
            "valid_from", "valid_to", "main_shape_id", "trip_headsign",
            "parent_version_id", "note"
        ])
        # valid_from and valid_to be converted to datetime
        route_versions_df['valid_from'] = pd.to_datetime(route_versions_df['valid_from'])
        route_versions_df['valid_to'] = pd.to_datetime(route_versions_df['valid_to'])
        
        ### shape_variants_df ###
        shape_variants_df = pd.DataFrame(columns=[
            "shape_variant_id", "version_id", "shape_id", "trip_headsign", "is_main", "note"
        ])

        ### shape_variant_activations_df ###
        shape_variant_activations_df = pd.DataFrame(columns=[
            "date", "shape_variant_id", "exception_type"
        ])
        #shape_variant_activations_df["exception_type"]
        shape_variant_activations_df.astype({"exception_type": "float64"})

        ### temporary_changes_df ###
        temporary_changes_df = pd.DataFrame(columns=[
            "detour_id", "route_id", "start_date", "end_date", "affects_version_id", "description"
        ])
        # Save
        shapes_df.to_csv(shapes_df_path, index=False)
        routes_df.to_csv(routes_df_path, index=False)
        route_versions_df.to_csv(route_versions_df_path, index=False)
        shape_variants_df.to_csv(shape_variants_df_path, index=False)
        shape_variant_activations_df.to_csv(shape_variant_activations_df_path, index=False)
        temporary_changes_df.to_csv(temporary_changes_df_path, index=False)

    return shapes_df, routes_df, route_versions_df, shape_variants_df, shape_variant_activations_df, temporary_changes_df


def get_active_dates(df, service_id, to_string=True, date_format='%Y-%m-%d'):
    """
    Get all active dates for a service.
    
    Args:
        df: DataFrame with service data
        service_id: ID of the service to look up
        to_string: Convert dates to strings
        date_format: Format for string conversion
    
    Returns:
        List of all active dates for the service
    """
    # Filter for the specific service_id
    service_row = df[df['service_id'] == service_id]
   
    if service_row.empty:
        print(f"Service ID '{service_id}' not found")
        return []
   
    # Get the first (and should be only) row
    service = service_row.iloc[0]
    start_date = pd.to_datetime(service['start_date'], format='%Y%m%d')
    end_date = pd.to_datetime(service['end_date'], format='%Y%m%d')
   
    # Days of week mapping (Monday=0, Sunday=6)
    day_columns = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    days_active = [i for i, day_col in enumerate(day_columns) if service[day_col] == 1]
   
    if not days_active:
        print(f"Service ID '{service_id}' has no active days")
        return []
   
    # Generate all dates in the range that fall on active days
    active_dates = []
    current_date = start_date
   
    while current_date <= end_date:
        # Check if current date's weekday is in our active days
        if current_date.weekday() in days_active:
            active_dates.append(current_date)
        current_date += timedelta(days=1)
   
    if to_string:
        active_dates = [date.strftime(date_format) for date in active_dates]
    return active_dates


def trips2latest_routes(trips_df, trip_first_date, routes_txt):
    extended_trips = trips_df.copy()
    extended_trips["first_date"] = extended_trips["service_id"].map(trip_first_date)
    extended_trips = extended_trips.copy()
    extended_trips = extended_trips[["service_id", "route_id", "shape_id", "trip_headsign", "direction_id", "first_date"]]
    extended_trips = extended_trips.groupby(["route_id", "shape_id", "trip_headsign", "direction_id", "first_date"]).count().reset_index()
    extended_trips = extended_trips.sort_values(by=['route_id', 'direction_id', 'service_id'], ascending=[True, True, False])
    extended_trips = extended_trips.drop_duplicates(subset=['route_id', 'direction_id'], ignore_index=True)
    extended_trips = extended_trips.rename(columns={"shape_id" : "main_shape_id", "first_date" : "valid_from"})
    
    latest_routes_df = pd.merge(
        routes_txt,
        extended_trips[["route_id", "main_shape_id", "trip_headsign", "direction_id", "valid_from"]],
        on="route_id",
        how="inner",)
    
    return latest_routes_df


def update_routes_df(routes_df, latest_routes_df):
    # Use relevant columns, without route_desc
    cols_to_use = [col for col in routes_df.columns]

    # Select new rows - rows whats route_id is not in routes_df 
    new_routes = latest_routes_df[~latest_routes_df["route_id"].isin(routes_df["route_id"])][cols_to_use]
    
    # Concatenate new routes
    updated_routes_df = pd.concat([routes_df, new_routes], ignore_index=True)

    # Check for duplicates
    duplicates = updated_routes_df[updated_routes_df.groupby("route_id")["route_id"].transform("count") > 2]

    if not duplicates.empty:
        print(f"Warning: There are {duplicates['route_id'].nunique()} duplicated route_id(s) in routes_df!")
        print("Duplicated route_id(s):")
        print(duplicates['route_id'].unique())
    else:
        print("No duplicate route_id found in routes_df.")

    return updated_routes_df

def save_routes(routes_df, data_folder):
    _, routes_df_path, _, _, _, _ = define_paths(data_folder=data_folder)
    routes_df.to_csv(routes_df_path, index=False)
    print(f"routes_df saved to {routes_df_path}")


def version_exists(current_versions, row):
    return (
        ((current_versions["route_id"] == row["route_id"]) &
         (current_versions["direction_id"] == row["direction_id"]) &
         (current_versions["main_shape_id"] == row["main_shape_id"]) &
         (current_versions["trip_headsign"] == row["trip_headsign"]))
        .any()
    )

def update_route_versions(route_versions_df, latest_routes_df, date):
    route_versions_copy_df = route_versions_df.copy()
    # version_id starting point
    START_VERSION_ID = 100_000

    # If the file is empty
    if route_versions_df.empty:
        next_version_id = START_VERSION_ID
    else:
        next_version_id = route_versions_df["version_id"].max() + 1

    # Create a new versions dataframe
    new_versions_df = latest_routes_df.copy()[["route_id", "main_shape_id", "trip_headsign", "direction_id", "route_desc", "valid_from"]]
    new_versions_df["valid_from"] = pd.to_datetime(new_versions_df['valid_from'])
    new_versions_df["valid_to"] = pd.NaT
    new_versions_df["parent_version_id"] = np.nan
    new_versions_df["note"] = np.nan

    # Define the current versions
    current_versions = route_versions_df[route_versions_df["valid_to"].isna()]

    # Let only the new versions
    new_versions_filtered = new_versions_df[~new_versions_df.apply(lambda row: version_exists(row, current_versions), axis=1)].copy()

    # Update the previous versions valid_to date
    for _, row in new_versions_filtered.iterrows():
        mask = (
            (route_versions_df["route_id"] == row["route_id"]) &
            (route_versions_df["valid_to"].isna())
        )
        route_versions_copy_df.loc[mask, "valid_to"] = row["valid_from"] - pd.Timedelta(days=1)

    new_versions_filtered["version_id"] = range(next_version_id, next_version_id + len(new_versions_filtered))

    # Concat
    extended_route_versions_df = pd.concat([route_versions_copy_df, new_versions_filtered], ignore_index=True)

    return extended_route_versions_df

def save_route_versions(route_versions_df, data_folder):
    _, _, route_versions_df_path, _, _, _ = define_paths(data_folder=data_folder)
    route_versions_df.to_csv(route_versions_df_path, index=False)
    print(f"routes_df saved to {route_versions_df_path}")


def get_df_with_noexception(trip_dates, trips_df):
    non_empty_keys = [key for key, value in trip_dates.items() if value]

    inservice_df = trips_df[trips_df["service_id"].isin(non_empty_keys)]
    inservice_df = inservice_df[["service_id", "route_id", "shape_id", "trip_headsign", "direction_id"]]
    inservice_df = inservice_df.groupby(["route_id", "shape_id", "trip_headsign", "direction_id"]).agg("first").reset_index()

    # First add the list column
    inservice_df['date_list'] = inservice_df['service_id'].map(trip_dates)

    # Then explode the list column into separate rows
    df_noexceptions = inservice_df.explode('date_list')
    df_noexceptions = df_noexceptions.rename(columns={'date_list': 'date'})
    df_noexceptions.drop(columns=['service_id'], inplace=True)
    df_noexceptions['exception_type'] = np.nan
    
    return df_noexceptions


def get_df_with_exception(calendar_dates_df, trips_df):
    calendar_dates_df['date'] = pd.to_datetime(calendar_dates_df['date'], format="%Y%m%d")

    extra_service_ids = calendar_dates_df[["date", "service_id", "exception_type"]].copy()
    extra_service_ids["exception_type"] = extra_service_ids["exception_type"].astype(int)

    df_exceptions = pd.merge(trips_df, extra_service_ids, how="left", on="service_id")
    df_exceptions = df_exceptions.groupby(["route_id", "shape_id", "trip_headsign", "direction_id", "date"]).agg('first').reset_index()
    df_exceptions = df_exceptions[["date", "route_id", "shape_id", "trip_headsign", "direction_id", "exception_type"]]

    # This will actually convert 2.0 to "2" and keep NaN as NaN
    df_exceptions["exception_type"] = df_exceptions["exception_type"].apply(
        lambda x: str(int(x)) if pd.notna(x) else x
    )

    return df_exceptions


def merge_exception_dfs(df_noexceptions, df_exceptions):
    df1 = df_noexceptions.copy()
    df2 = df_exceptions.copy()

    # Egységes date formátum mindkettőben
    df1['date'] = pd.to_datetime(df1['date']).dt.strftime('%Y-%m-%d')
    df2['date'] = pd.to_datetime(df2['date']).dt.strftime('%Y-%m-%d')

    # Összefűzzük a kettőt
    combined = pd.concat([df1, df2], ignore_index=True)

    # Az oszlopok, amelyek alapján duplikációt nézünk (exception_type nélkül)
    cols_except_exception = [col for col in combined.columns if col != 'exception_type']

    # Megjelöljük, hogy az exception_type NaN-e
    combined['__is_nan__'] = combined['exception_type'].isna()

    # Rendezés: nem-NaN előre
    combined_sorted = combined.sort_values('__is_nan__')

    # Duplikátumok kiszűrése
    before = len(combined_sorted)
    merged_df = combined_sorted.drop_duplicates(subset=cols_except_exception, keep='first')
    after = len(merged_df)
    removed = before - after

    # Segédoszlop eltávolítása
    merged_df = merged_df.drop(columns='__is_nan__')

    # Rendezés a megadott oszlopok szerint
    sort_columns = ['date', 'route_id', 'direction_id', 'shape_id', 'trip_headsign', 'exception_type']
    merged_df = merged_df.sort_values(by=sort_columns)

    # Index újraszámozása
    merged_df = merged_df.reset_index(drop=True)

    # Jelentés
    print(f"{removed} duplikált sort eltávolítottunk, ahol csak az exception_type tért el (NaN vs nem-NaN).")
    return merged_df

def get_extended4shape_variants(extended_route_versions_df, df_noexceptions, df_exceptions):
    valid_routes = extended_route_versions_df[extended_route_versions_df["valid_to"].isna()][["version_id", "route_id", "direction_id", "main_shape_id"]]

    merged_df = merge_exception_dfs(df_noexceptions, df_exceptions)
    return_df = pd.merge(valid_routes, merged_df, on=["route_id", "direction_id"])
    return_df["main_shape_id"] = (return_df["main_shape_id"] == return_df["shape_id"]).astype(int)
    return_df = return_df.rename(columns={"main_shape_id" : "is_main"})
    return return_df



def update_shape_variants_and_activations(return_df, shape_variants_df, shape_variant_activations_df):
    # Get unique shape variants from merged_df
    new_variants = return_df[['version_id', 'shape_id', 'trip_headsign', 'is_main']].drop_duplicates().reset_index(drop=True)

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
            start_id = 100000
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

    # Merge merged_df with variant mapping to get shape_variant_id for each row
    merged_with_variant_id = return_df.merge(
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
    #print(shape_variants_df.head(10))
    print(f"Shape: {shape_variants_df.shape}")
    print()

    print("Updated shape_variant_activations_df:")
    #print(shape_variant_activations_df.head(10))
    print(f"Shape: {shape_variant_activations_df.shape}")

    # Summary
    print(f"\nSummary:")
    print(f"Total unique shape variants: {len(shape_variants_df)}")
    print(f"Total shape variant activations: {len(shape_variant_activations_df)}")
    if not truly_new_variants.empty:
        print(f"New variants added: {len(truly_new_variants)}")
        print(f"Shape variant IDs added: {truly_new_variants['shape_variant_id'].min()} - {truly_new_variants['shape_variant_id'].max()}" if 'shape_variant_id' in locals() else "")
    else:
        print("No new variants added")
    if not truly_new_activations.empty:
        print(f"New activations added: {len(truly_new_activations)}")
    else:
        print("No new activations added")

    return shape_variants_df, shape_variant_activations_df


def save_shape_variants_df(shape_variants_df, data_folder):
    _, _, _, shape_variants_df_path, _, _ = define_paths(data_folder=data_folder)
    shape_variants_df.to_csv(shape_variants_df_path, index=False)
    print(f"shape_variants_df saved to {shape_variants_df_path}")

def save_shape_variant_activations_df(shape_variant_activations_df, data_folder):
    _, _, _, _, shape_variant_activations_df_path, _ = define_paths(data_folder=data_folder)
    shape_variant_activations_df.to_csv(shape_variant_activations_df_path, index=False)
    print(f"shape_variant_activations_df saved to {shape_variant_activations_df_path}")