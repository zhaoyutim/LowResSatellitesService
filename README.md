### Web Clients for downloading and processing raw VIIRS satellite images from LAADS DAAC

Key Dependencies: GDAL, Satpy

main.py: entry of this project \
LaadsDataHandler/laads_client.py: LAADS client to download satellite images from LAADS DAAC. Key words support: day_night, product_id, collection_id, roi \
ProcessingPipeline/processing_pipeline.py: Whole processing pipeline consists of 1) reading and projection of raw satellite images, 2) cloud optimization 3) roi crop 4) upload to gcloud 5) upload from gcloud to gee \
roi: all the study areas used.

Current Known Problem: Connection refuse from LAADS side, you can try to rerun the script. \




