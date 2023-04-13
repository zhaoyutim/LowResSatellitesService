### Web Clients for downloading and processing raw VIIRS satellite images from LAADS DAAC

## Repo Structure
    .
    ├── LaadsDataHandler                                     # Client for NASA LAADS Distributed Active Archive Center 
    │   ├── laads_client.py                                  # Client to request product list json given roi and date 
    ├── ProcessingPipeline                                   # Figures used in README 
    ├── roi                                                  # Region of Interest Folder
    │   ├── *.csv                                            # Region of Interest with fire_id, start_date end_date and coordinates 
    ├── convert_csv.py                                       # Script to convert csv downloaded from GEE script to desired format
    ├── main_large_scale.py                                  # Download NetCDF files according to product list json in parallel
    ├── main_processing_pipepline.py                         # Main processing pipeline to project, subset the dataset  
    ├── sanity_check.py                                      # Sanity check on the downloaded NetCDF files and auto-removal, Sanity Check on the GEE upload
    ├── upload_to_gee.py                                     # Upload to GEE
    ├── main_mosaic.py                                       # Mosaic given Geotiffs
    └── README.md


This repo intends to download VIIRS Level-1 product from NASA LAADS DAAC.\
User has to first run main_large_scale.py by feeding the collection_id product_id and the list of fires to download.\
After the NetCDF files are downloaded locally, User has to run main_processing_pipeline.py to process the file to geotiff \
and subset them to image patches on fire roi.\
Key Dependencies: GDAL, Satpy, earthengine-api


