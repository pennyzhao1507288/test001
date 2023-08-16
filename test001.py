import numpy as np
import xarray as xr
import pandas as pd
import dask.array as da
from scipy import interpolate
from pathlib import Path

# Load the original NetCDF file using dask
lazy_data = xr.open_dataset(r'X:\zp\ncfile\pr_day_EC-Earth3_ssp585_r1i1p1f1_gr_20230101-20231231.nc', chunks={'time': 10, 'lat': 100, 'lon': 100})['pr']

# Define new latitude and longitude arrays
new_lon = np.arange(112.5, 129.4, 0.01)
new_lat = np.arange(31.23, 45.96, 0.01)

# Interpolate data to the new resolution
new_data = lazy_data.interp(lat=new_lat, lon=new_lon)

# Perform operations on the lazy array when needed
pr_subset = new_data.sel(lat=slice(30.5263, 45.9648), lon=slice(111.797, 130.078))

# Compute the result when needed
pr_subset = pr_subset.load()

# Extract precipitation data for the 12 specific locations
station_coords = [
    (495, 565), (435, 633), (382, 585), (362, 557),
    (364, 507), (430, 597), (455, 618), (427, 512),
    (449, 542), (465, 528), (442, 480), (419, 448)
]

station_data = {}
for row, col in station_coords:
    station_data[f'{row}_{col}'] = pr_subset[:, row, col].values

# Convert the extracted data to a pandas DataFrame
df = pd.DataFrame(station_data)

# Save the DataFrame to an Excel file
output_path = r'X:\zp\ncfile\pr_day_EC-Earth3_station_data2023.xlsx'
df.to_excel(output_path, index=True)
print(f'Data saved to {output_path}')
