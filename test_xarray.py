import xarray as xr
vnp02_path = '/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/data/VNPNC/2023-04-03/D/0542/VNP02IMG.A2023093.0542.002.2023093125752.nc'
vnp03_path = '/geoinfo_vol1/home/z/h/zhao2/LowResSatellitesService/data/VNPNC/2023-04-03/D/0542/VNP03IMG.A2023093.0542.002.2023093123512.nc'


if __name__ == '__main__':
    with xr.open_dataset(vnp02_path) as ds:
        ds['I01']