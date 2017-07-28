import os

import numpy as np
import pandas as pd

from astropy.coordinates import SkyCoord
from astropy.coordinates import match_coordinates_sky
from astropy import units as u

# First read analysis DataFrames
path = '/global/cscratch1/sd/cwalter/'

with pd.HDFStore(path + 'analysis-dataframes.h5') as f:
    analysis = f['analysis']

with pd.HDFStore(path + 'truth-dataframes.h5') as f:
    stars = f['stars']
    galaxies = f['galaxies']

# Make combined catalaog
stars['comb_mag'] = stars.r_mag
stars.set_index(['id'], inplace=True)

stars['object_class'] = "stars"
galaxies['object_class'] = "galaxies"

combined_catalog = pd.concat([stars, galaxies])

combined_catalog.reset_index(inplace=True, drop=True)
analysis.reset_index(inplace=True, drop=True)

# Get numpy vectors
sky_catalog_ra = combined_catalog.raICRS.values
sky_catalog_dec = combined_catalog.decICRS.values
sources_ra = analysis.coord_ra.values
sources_dec = analysis.coord_dec.values

sky_catalog = SkyCoord(sky_catalog_ra*u.degree,
                       sky_catalog_dec*u.degree, frame='icrs')
sources = SkyCoord(sources_ra*u.radian, sources_dec*u.radian,
                   frame='icrs')

# Do catalog matching
print("Begining Catalog Match")
index, dist2d, dist3d = match_coordinates_sky(sources, sky_catalog,
                                              nthneighbor=1, storekdtree=False)

analysis['match_index'] = index
analysis['distance'] = dist2d.arcsec

# Join matched catalogs
print("Begining DataFrame Join")
matched = analysis.join(combined_catalog, on='match_index', how='left')

# Now write out DataFrames
scratch_disk = os.getenv('CSCRATCH')
file_name = 'matched-dataframes.h5'

with pd.HDFStore(scratch_disk + '/' + file_name, mode='w') as f:
    f['matched'] = matched

