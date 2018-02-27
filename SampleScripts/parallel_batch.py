# batch square root calculations run in parallel

# import necessary modules
import arcpy
import os
import time
import logging
from multiprocessing import Process, Queue, Pool, \
    cpu_count, current_process, Manager

# set global arcpy environments
arcpy.env.overwriteOutput = True
arcpy.CheckOutExtension("Spatial")
current_dir = os.getcwd()

# create a logger to report
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

""" 
Set path to input raster workspace.
This is the relative path where a collection of input rasters to be processed is located.
"""
in_raster_dir = os.path.join(current_dir, r'input_raster_workspace')
# set relative path to the output workspace where output results from batch processing are stored.
out_raster_dir = os.path.join(current_dir, r'sqrt_output')

# set workspace to raster workspace
arcpy.env.workspace = in_raster_dir
arcpy.env.scratchWorkspace = "in_memory"


""" 
Worker function that would calculate square root i.e. consumer function.
"""
def calculate_sqrt(q):
    while not q.empty():
        ras = q.get(True, 0.05)
        logger.info(
            "Running Square Root tool by: {} with PID {}".format(current_process().name, os.getpid()))
        out_sqrt_name = os.path.basename(ras).split('.')[0] + '_fnl.tif'
        out_sqrt_path = os.path.join(out_raster_dir, out_sqrt_name)
        sqrt_ras = arcpy.sa.SquareRoot(ras)
        sqrt_ras.save(out_sqrt_path)

"""
This is the producer function. This function adds a list of rasters into a FIFO queue.
"""
def producer_task(q, list_of_ras):
    for ras in list_of_ras:
        logger.info("Producer [%s] putting value [%s] into queue.. "
                    % (current_process().name, str(os.path.basename(ras))))
        q.put(ras)


def main():
    list_of_ras = arcpy.ListRasters()
    # create an enpty FIFO queue
    data_queue = Queue()
    # Create a producer task and kick it off
    producer = Process(target=producer_task, args=(data_queue, list_of_ras))
    producer.start()
    producer.join()

    consumer_list = []
    number_of_cpus = cpu_count()
    for i in range(number_of_cpus):
        consumer = Process(target=calculate_sqrt, args=(data_queue,))
        consumer.start()
        consumer_list.append(consumer)

    [consumer.join() for consumer in consumer_list]


if __name__ == '__main__':
    time_start = time.clock()
    main()
    time_end = time.clock()
    logger.info("Time taken in main in seconds(s) is : {}".format(str(time_end - time_start)))
