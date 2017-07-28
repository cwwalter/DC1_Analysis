import os
import numpy as np
import pandas as pd
import dask.dataframe as dd

def analysis_flags(FLAGS1, FLAGS2, FLAGS3, bit_list):
    """
    A function to repaste the three FLAG integers backtogether and
    recover the analysis flag bit mask. Returns a boolean array with the values
    of the bits specified in the input list.
    """
    
    string = '{:064b}'.format(FLAGS1)[::-1] + '{:064b}'.format(FLAGS2)[::-1] \
           + '{:014b}'.format(FLAGS3)[::-1]
        
    bit_mask = np.array(list(string), dtype=np.int).astype(np.bool)
    
    return np.array([bit_mask[bit] for bit in bit_list])


def frame_loop(FLAGS1, FLAGS2, FLAGS3, bit_list):
    '''
    A function that will loop over the dataframe and return numpy
    arrays of the flags.
    '''
    
    is_primary = np.empty([FLAGS1.size, bit_list.size], dtype=np.bool)

    for i in range(FLAGS1.size):
        is_primary[i] = analysis_flags(FLAGS1[i], FLAGS2[i], FLAGS3[i],
                                       bit_list)
     
    return is_primary


# First read the Dask file and make a normal DataFrame
file_path = "/global/project/projectdirs/lsst/cwalter/DC1-HDF5/"
df = dd.read_hdf(file_path+'Coadd_dithered.hdf', key='/*')

requested_columns = ['FLAGS1','FLAGS2','FLAGS3','patch', 'footprint',
                     'coord_ra','coord_dec', 
                     'base_PsfFlux_flux','base_PsfFlux_fluxSigma',
                     'modelfit_CModel_flux']

# You can use the commented out line instead for testing.  It gets the
# first 1000 lines.
selected = df[requested_columns].compute()
#selected = df[requested_columns].head(1000, npartitions=10, compute=True)

# Because of the way Dask works the index in the DataFrame will be
# restarted on each key.  This renumbers them all sequentially.  This
# is necessary or, when we try to do concatanation and joins later, it
# won't work since there are duplicated index values.
selected.reset_index(inplace=True, drop=True)

# First we want to add a few more variables we might need for analysis
flux_mag0 = 63095734448.0194  # Zero point for magnitudes

modified = selected.assign(psfMag = lambda x:
                           -2.5*np.log10(x.base_PsfFlux_flux/flux_mag0)) \
                   .assign(cmodelMag = lambda x:
                           -2.5*np.log10(x.modelfit_CModel_flux/flux_mag0)) \
                   .assign(extendedness = lambda x: x.psfMag - x.cmodelMag)

# Now we want to unpack the analyis flags and add new flag columns to
# the DataFrame.
#
# Thes are the flags to look up and add. Note you need to subtract one
# from these before you check the bit mask as the actaul bits are
# labeled starting at zero.
#
# TFLAG95 = 'base_PixelFlags_flag_interpolatedCenter'  
# TFLAG96 = 'base_PixelFlags_flag_saturatedCenter'
#
# TFLAG99 = 'base_PsfFlux_flag'
# TFLAG116= 'modelfit_CModel_flag'
#
# TFLAG129= 'detect_isPrimary' 

TFLAG_LIST = np.array([95, 96, 99, 116, 129])

new_column_list = ['base_PixelFlags_flag_interpolatedCenter',
                   'base_PixelFlags_flag_saturatedCenter',
                  'base_PsfFlux_flag', 'modelfit_CModel_flag',
                   'detect_isPrimary']

# Loop over the dataframe and put the resulting numpy flag arrays into
# a new dataframe. Note the subtraction of one from the TFLAG_LIST.
flag_frame = pd.DataFrame(frame_loop( modified.FLAGS1.values,
                                      modified.FLAGS2.values,
                                      modified.FLAGS3.values,
                                      TFLAG_LIST-1)
                          ,columns=new_column_list)

# Now that we don't need them anymore, get rid of the FLAG integers
# from the database
modified.drop(['FLAGS1','FLAGS2','FLAGS3'], axis=1, inplace=True)

# Concatonate the new dataframe with the original data frame
analysis = pd.concat([modified, flag_frame], axis=1)

# Now write out DataFrames
scratch_disk = os.getenv('CSCRATCH')
file_name = 'analysis-dataframes.h5'

with pd.HDFStore(scratch_disk + '/' + file_name, mode='w') as f:
    f['analysis'] = analysis
