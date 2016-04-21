from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection
from matplotlib.patches import PathPatch
import numpy as np

blue = "#B2D5FF"
brown = "#F0EDE5"
green = "#D2E4C8"
red = "#EBDED4"
black = "#363F38"
gray = "#999999"
white = "#FFFFFF"

fig     = plt.figure()
ax      = fig.add_subplot(111)

map = Basemap()

map.drawmapboundary(fill_color=white)
map.fillcontinents(color=gray ,lake_color=white)
#map.drawcoastlines(color=black)

#map.readshapefile('./Historic/cntry1920', 'cntry1920')
map.readshapefile('./Historic/cntry1920', 'cntry1920', drawbounds = False)

country = "Mexico"

for info, shape in zip(map.cntry1920_info, map.cntry1920):
    if country in info['NAME']:
        #x, y = zip(*shape) 
        #map.plot(x, y, marker=None,color='m')
        polygons = list()
        polygon = Polygon(np.array(shape), True)
        polygons.append(polygon)
        polygon_collection = PatchCollection(polygons, facecolor= 'm', edgecolor='k', linewidths=1, zorder=2)
        ax.add_collection(polygon_collection)
        break
        
plt.show()

