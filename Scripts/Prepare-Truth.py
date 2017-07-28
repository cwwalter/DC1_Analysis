import os
import pandas as pd
import dask.dataframe as dd
import numba
import math

# A function to add magnitudes to combine disk and bulges.
@numba.jit(nopython=True)
def fast_sum(values):
    '''
    Combine the magnitudes from all the components of a galaxy.  These
    will be passed as a column from the Pandas grouping function.
    '''
    sum = 0
    
    for value in values:
        mag = 10**(-0.4*value)
        sum = sum + mag

    return -2.5*math.log10(sum)


# First read the Dask file and make a normal DataFrame
file_path = '/global/project/projectdirs/lsst/cwalter/DC1-HDF5/'
stars_dask  = dd.read_hdf(file_path+'StarTruth.hdf', key='/*')
galaxy_dask = dd.read_hdf(file_path+'GalaxyTruth.hdf', key='/*')

stars = stars_dask[['id', 'raICRS', 'decICRS', 'r_mag']].compute()

galaxies = galaxy_dask[['id', 'raICRS', 'decICRS', 'r_mag',
                        'redshift', 'majorAxis', 'minorAxis',
                        'positionAngle', 'sindex']].compute()

# Now combine galaxy components
mask = 0b1111111111 # Lower 10 bits encode type

grouped_galaxies = galaxies.assign(new_id = lambda x: x.id.values >> 10) \
                           .assign(sub_type = lambda x: x.id.values & mask) \
                           .groupby('new_id')

combined_galaxies = grouped_galaxies.first()
combined_galaxies['num_components'] = grouped_galaxies.size()
combined_galaxies['comb_mag'] = grouped_galaxies.r_mag.agg(lambda x:
                                                           fast_sum(x.values))

# Get rid of columns we don't need anymore
combined_galaxies.drop(['id'], axis=1, inplace=True)

# Now write out DataFrames
scratch_disk = os.getenv('CSCRATCH')
file_name = 'truth-dataframes.h5'

with pd.HDFStore(scratch_disk + '/' + file_name, mode='w') as f:
    f['stars'] = stars
    f['galaxies'] = combined_galaxies
