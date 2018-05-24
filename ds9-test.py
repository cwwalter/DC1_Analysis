import lsst.daf.persistence
import lsst.afw.display as afw_display

DATA_DIR_DITHERED = "/global/cscratch1/sd/descdm/DC1/DC1-imsim-dithered/"

butler = lsst.daf.persistence.Butler(DATA_DIR_DITHERED)

calexpId = {'visit':1919421, 'filter':'r', 'raft':'2,2', 'sensor':'1,1'}
coaddId = {'patch':'17,13', 'tract':0, 'filter':'r'}

calexp = butler.get('calexp', dataId=calexpId)
calexp_src = butler.get('src', dataId=calexpId)
coadd = butler.get('deepCoadd', dataId=coaddId)
coadd_src = butler.get('deepCoadd_meas', dataId=coaddId)

catalog_imsim = butler.get("deepCoadd_ref", patch='17,13', tract=0,
                           immediate=True)                                                                      
calexp_display = afw_display.getDisplay(frame=1)
coadd_display = afw_display.getDisplay(frame=2)

calexp_display.mtv(calexp)
coadd_display.mtv(coadd)

with coadd_display.Buffering():     
         for source in catalog_imsim:
                 coadd_display.dot('o', source.getX(), source.getY())

# calexp_cmodelFlux =  calexp_src.getModelFlux()
# print("calexp source length = ", len(calexp_cmodelFlux))

# calexp_calib = calexp.getCalib()
# calexp_calib.getMagnitude(calexp_cmodelFlux)
