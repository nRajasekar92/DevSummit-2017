"""parallel processing raster tools that use a local neighborhood for analysis 
i.e. output at any cell depends on input value(s) at the cell.
"""
#import necessary modules
import arcpy
import os
import time
import logging
from multiprocessing import Process, Queue, Pool, \
    cpu_count, current_process, Manager

#set global arcpy environments
arcpy.env.overwriteOutput = True
arcpy.CheckOutExtension("Spatial")
arcpy.env.scratchWorkspace = "in_memory"

#create a logger to report
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

#describe paths
in_raster_path = os.path.join(os.getcwd(), r'input_raster.tif')
out_fishnet_path = os.path.join(os.getcwd(), r'fishnets', r'fishnet.shp')

"""
A function used to create a fishnet feature for the input raster's extents. 
Using this function, you can decide how to chunk the input dataset so that you may process it parallely.
"""
def create_fishnet(in_raster_path, out_fc_path):
	#create raster object from in_raster_path
	ras1 = arcpy.Raster(in_raster_path)
	#specify input parameters to fishnet tool
	XMin = ras1.extent.XMin
	XMax = ras1.extent.XMax
	YMin = ras1.extent.YMin
	YMax = ras1.extent.YMax
	origCord = "{} {}".format(XMin, YMin)
	YAxisCord = "{} {}".format(XMin, YMax)
	CornerCord = "{} {}".format(XMax, YMax)
	cellSizeW = '0'
	cellSizeH = '0'
	"""modify the numRows and numCols to control how the input raster is chunked for parallel processing
	For example, if numRows = 4 & numCols = 4, total number of chunks = 4*4 = 16. 
	"""
	numRows = 4
	numCols = 4
	geo_type = "POLYGON"
	#Run fishnet tool
	logger.info("Running fishnet creator: {} with PID {}".format(current_process().name, os.getpid()))
	arcpy.env.outputCoordinateSystem = ras1.spatialReference
	arcpy.CreateFishnet_management(out_fc_path, origCord, YAxisCord, cellSizeW, cellSizeH, numRows, numCols, CornerCord, "NO_LABELS", "", geo_type)
	arcpy.ClearEnvironment("outputCoordinateSystem")

"""
Worker Function that executes a Local function in a parallel fashion.
This function takes as input, an item in a dictionary of extents. 
Each item in this dictionary corresponds to the extent for a given chunk that has to be processed.
"""
def execute_task(in_extentDict):
	#start the clock
	time1 = time.clock()
	#get extent count and extents
	fc_count = in_extentDict[0]
	procExt = in_extentDict[1]
	XMin = procExt[0]
	YMin = procExt[1]
	XMax = procExt[2]
	YMax = procExt[3]
	# set environments
	arcpy.env.snapRaster = in_raster_path
	arcpy.env.cellsize = in_raster_path
	arcpy.env.extent = arcpy.Extent(XMin, YMin, XMax, YMax)
	#send process info to logger
	logger.info("Running local math task: {} with PID {}".format(current_process().name, os.getpid()))
	#run the local task
	ras_out = arcpy.sa.SquareRoot(in_raster_path)
	#clear the extent environment
	arcpy.ClearEnvironment("extent")
	#specify output path and save it
	out_name = "out_sqrt_ras{}.tif".format(fc_count)
	out_path = os.path.join(os.getcwd(), r"local_rast_wspace", out_name)
	ras_out.save(out_path)
	#end the clock
	time2 = time.clock()
	logger.info("{} with PID {} finished in {}".format(current_process().name, os.getpid(), str(time2-time1)))

if __name__ == '__main__':
	#start the clock
	time_start = time.clock()
	# call create fishnet function
	logger.info("Creating fishnet features..")
	create_fishnet(in_raster_path,out_fishnet_path)
	#get extents of individual chunk features in the fishnet, add the extents to a dictonary
	extDict = {}
	count = 1
	for row in arcpy.da.SearchCursor(out_fishnet_path, ["SHAPE@"]):
		extent_curr = row[0].extent
		ls = []
		ls.append(extent_curr.XMin)
		ls.append(extent_curr.YMin)
		ls.append(extent_curr.XMax)
		ls.append(extent_curr.YMax)
		extDict[count] = ls
		count+=1
	# create a process pool and pass dictonary of extent to execute task
	pool = Pool(processes=cpu_count())
	pool.map(execute_task, extDict.items())
	pool.close()
	pool.join()
	#add results to mosaic dataset
	arcpy.env.workspace = os.getcwd()
	in_path = os.path.join(os.getcwd(), r"local_rast_wspace")
	arcpy.AddRastersToMosaicDataset_management("mosaic_out.gdb\localFn_Mosaic", "Raster Dataset", in_path)
	#end the clock
	time_end = time.clock()
	logger.info("Time taken in main in seconds(s) is : {}".format(str(time_end-time_start)))
