from sentinelhub import DataCollection


class Sentinel2:
    def __init__(self, mode):
        if mode == "TIR":
            self.resolution = 20
            self.collection = DataCollection.SENTINEL2_L2A
            self.band_names = ["S7", "S8", "F1", "F2"]
            self.units = "DN"
            self.pixel_scale = 1
