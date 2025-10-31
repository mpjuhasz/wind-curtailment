import pyproj

osgb36 = pyproj.CRS("EPSG:27700")
wgs84 = pyproj.CRS("EPSG:4326")

# Most UK shapefiles use osgb36
transformer = pyproj.Transformer.from_crs(osgb36, wgs84, always_xy=True)


def bng_xy_to_lat_long(x: float, y: float) -> tuple[float, float]:
    return transformer.transform(x, y)