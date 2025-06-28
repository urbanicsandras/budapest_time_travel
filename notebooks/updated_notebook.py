# Updated notebook using the new modular structure
# preprocessing_v10.ipynb

# Cell 1: Imports
import pandas as pd
import numpy as np
import os
import sys

# Set path - adjust according to your new package location
project_root = os.path.abspath(os.path.join("..", "src"))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import the new modular package
from transit_data_processor import process_transit_data, TransitDataProcessor

# Cell 2: Simple processing using the convenience function
date = '20131018'
data_folder = '../data/processed/'

# Process the data - this replaces all the individual function calls
result = process_transit_data(date, data_folder, save_data=True)

# Cell 3: Access the results if needed for further analysis
shapes_df = result['shapes']
routes_df = result['routes']
route_versions_df = result['route_versions']
shape_variants_df = result['shape_variants']
shape_variant_activations_df = result['shape_variant_activations']
temporary_changes_df = result['temporary_changes']

print(f"Routes: {routes_df.shape}")
print(f"Route versions: {route_versions_df.shape}")
print(f"Shape variants: {shape_variants_df.shape}")
print(f"Shape variant activations: {shape_variant_activations_df.shape}")

# Cell 4: Alternative - using the class for more control
processor = TransitDataProcessor(data_folder)

# Process without saving (for testing)
result = processor.process_date(date, save_data=False)

# Or process multiple dates in a loop
dates_to_process = ['20131018', '20131019', '20131020']
for date in dates_to_process:
    print(f"\nProcessing {date}...")
    processor.process_date(date, save_data=True)

# Cell 5: Individual function usage (if you need more granular control)
from transit_data_processor import (
    load_gtfs_data, load_processed_data, 
    build_service_date_mappings, build_latest_routes
)

# Load data manually
routes_txt, trips_txt, shapes_txt, calendar_txt, calendar_dates_txt = load_gtfs_data(date)
processed_data = load_processed_data(data_folder)

# Build mappings
trip_dates, trip_first_date = build_service_date_mappings(trips_txt, calendar_txt)

# Continue with individual processing steps as needed...