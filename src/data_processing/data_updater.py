import pandas as pd
import numpy as np
import os

def define_paths(data_folder=None, date=None):
    if data_folder:
        routes_df_path = os.path.join(data_folder, 'routes.csv')
        route_versions_df_path = os.path.join(data_folder, 'route_versions.csv')
        temporary_changes_df_path = os.path.join(data_folder, 'temporary_changes.csv')
        return routes_df_path, route_versions_df_path, temporary_changes_df_path

    if date:
        date_folder = '../data/raw/'
        routes_path = os.path.join(date_folder, date, 'routes.txt')
        trips_path = os.path.join(date_folder, date, 'trips.txt')
        shapes_path = os.path.join(date_folder, date, 'shapes.txt')

        return routes_path, trips_path, shapes_path

    raise ValueError("Either data_folder or date must be provided.")

def load_txt_data(date, print_shapes=False):
    routes_path, trips_path, shapes_path = define_paths(date=date)
    routes_txt = pd.read_csv(routes_path)
    trips_txt = pd.read_csv(trips_path)
    shapes_txt = pd.read_csv(shapes_path)

    if print_shapes:
        print("Routes:", routes_txt.shape)
        print("Trips:", trips_txt.shape)
        print("Shapes:", shapes_txt.shape)
    return routes_txt, trips_txt, shapes_txt

def load_df_data(data_folder):
    routes_df_path, route_versions_df_path, temporary_changes_df_path = define_paths(data_folder=data_folder)

    try:
        routes_df = pd.read_csv(routes_df_path)
        #route_versions_df = pd.read_csv(route_versions_df_path)
        route_versions_df = pd.read_csv(route_versions_df_path, parse_dates=['valid_from', 'valid_to'])
        temporary_changes_df = pd.read_csv(temporary_changes_df_path)
    except FileNotFoundError:
        # Make empty dataframes for the first time
        routes_df = pd.DataFrame(columns=[
            "route_id", "agency_id", "route_short_name", "route_type", "route_color", "route_text_color"
        ])

        route_versions_df = pd.DataFrame(columns=[
            "version_id", "route_id", "direction_id", "route_long_name", "route_desc",
            "valid_from", "valid_to", "shape_id", "trip_headsign",
            "parent_version_id", "note"
        ])
        # valid_from and valid_to be converted to datetime
        route_versions_df['valid_from'] = pd.to_datetime(route_versions_df['valid_from'])
        route_versions_df['valid_to'] = pd.to_datetime(route_versions_df['valid_to'])
        
        temporary_changes_df = pd.DataFrame(columns=[
            "detour_id", "route_id", "start_date", "end_date", "affects_version_id", "description"
        ])
        # Save
        routes_df.to_csv(routes_df_path, index=False)
        route_versions_df.to_csv(route_versions_df_path, index=False)
        temporary_changes_df.to_csv(temporary_changes_df_path, index=False)

    return routes_df, route_versions_df, temporary_changes_df

def update_routes_df(routes_df, routes_txt):
    # Use relevant columns, without route_desc
    cols_to_use = [col for col in routes_txt.columns if col != "route_desc"]

    # Select new rows - rows whats route_id is not in routes_df 
    new_routes = routes_txt[~routes_txt["route_id"].isin(routes_df["route_id"])][cols_to_use]

    # Concatenate new routes
    updated_routes_df = pd.concat([routes_df, new_routes], ignore_index=True)

    # Check for duplicates
    duplicates = updated_routes_df[updated_routes_df.duplicated(subset="route_id", keep=False)]

    if not duplicates.empty:
        print(f"Warning: There are {duplicates['route_id'].nunique()} duplicated route_id(s) in routes_df!")
        print("Duplicated route_id(s):")
        print(duplicates['route_id'].unique())
    else:
        print("No duplicate route_id found in routes_df.")

    return updated_routes_df

def save_routes(routes_df, data_folder):
    routes_df_path, _, _ = define_paths(data_folder=data_folder)
    routes_df.to_csv(routes_df_path, index=False)
    print(f"routes_df saved to {routes_df_path}")

def version_exists(current_versions, row):
    return (
        ((current_versions["route_id"] == row["route_id"]) &
         (current_versions["direction_id"] == row["direction_id"]) &
         (current_versions["shape_id"] == row["shape_id"]) &
         (current_versions["trip_headsign"] == row["trip_headsign"]))
        .any()
    )

def update_route_versions(route_versions_df, trips_txt, routes_txt, date):
    route_versions_copy_df = route_versions_df.copy()
    # version_id starting point
    START_VERSION_ID = 100_000

    # If the file is empty
    if route_versions_df.empty:
        next_version_id = START_VERSION_ID
    else:
        next_version_id = route_versions_df["version_id"].max() + 1

    # Prepare new versions
    trips_grouped = trips_txt.groupby(['route_id', 'shape_id', 'trip_headsign', 'direction_id']).count()
    trips_grouped = trips_grouped.sort_values(by=['route_id', 'service_id'], ascending=[True, False])
    trips_grouped = trips_grouped.groupby('route_id').head(2).sort_values(by=['route_id', 'direction_id']).reset_index()
    trips_grouped = trips_grouped[["route_id", "shape_id", "trip_headsign", "direction_id"]]

    # Create a new versions dataframe
    new_versions_df = pd.merge(trips_grouped, routes_txt[["route_id", "route_long_name", "route_desc"]], on="route_id")
    new_versions_df["valid_from"] = pd.to_datetime(date)  # az adott GTFS snapshot dátuma
    new_versions_df["valid_to"] = pd.NaT
    new_versions_df["parent_version_id"] = np.nan
    new_versions_df["note"] = np.nan

    # Define the current versions
    current_versions = route_versions_df[route_versions_df["valid_to"].isna()]

    # Let only the new versions
    ##new_versions_filtered = new_versions_df[~new_versions_df.apply(version_exists, axis=1)].copy()
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
    _, route_versions_df_path, _ = define_paths(data_folder=data_folder)
    route_versions_df.to_csv(route_versions_df_path, index=False)
    print(f"routes_df saved to {route_versions_df_path}")