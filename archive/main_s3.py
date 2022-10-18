from Preprocessing.PreprocessingService import PreprocessingService
from LowResSatellitesService.LowResSatellitesService import LowResSatellitesService

if __name__=='__main__':
    location = 'kamloop'
    lowres = LowResSatellitesService()
    # lowres.fetch_imagery_from_sentinel_hub(location, ["S3"])
    lowres.fetch_imagery_from_sentinel_hub_custom(location, ["S2"])
    # preprocessing = PreprocessingService()
    # preprocessing.get_thresholded_image(location)