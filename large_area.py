import itertools

import numpy as np
from shapely.geometry import shape, Polygon, MultiPolygon, MultiLineString

from sentinelhub import BBoxSplitter, OsmSplitter, TileSplitter, CustomGridSplitter, UtmZoneSplitter, UtmGridSplitter
from sentinelhub import BBox, read_data, CRS, DataCollection

import matplotlib.pyplot as plt
from matplotlib.patches import Polygon as plt_polygon

from mpl_toolkits.basemap import Basemap  # Available here: https://github.com/matplotlib/basemap
