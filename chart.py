import logging
import gc
from data import Data
from datetime import datetime, date
from time import sleep

def waitUntil(hour):
    """Waits untill the hour specified"""
    ctime = datetime.now()
    if hour == 0:
        'If the time is tommorow, add one onto the day'
        ctime = datetime(ctime.year, ctime.month, ctime.day + 1, hour, 0, 0)
    else:
        ctime = datetime(ctime.year, ctime.month, ctime.day, hour, 0, 0)
        
    while ctime > datetime.now():
        'Sleeps untill the time is greater, waiting 30 seconds at a time'
        sleep(30)

'Gets the root logger'
logger = logging.getLogger()
'Sets the logger so anything of level info or greater gets output'
logger.setLevel(logging.INFO)

'Stops the logger from raising exceptions' 
logging.raiseExceptions = False

hour = datetime.now().hour + 1

while True:
    'Call a garbage collection to make syre there is no leftover data'
    gc.collect()

    waitUntil(hour)
    
    if len(logger.handlers) > 0:
        'If existing logging file open, close it'
        logger.handlers[0].stream.close()
        logger.removeHandler(logger.handlers[0])

    'Creates a new handler, that writes to a certian file (containing date)'
    filename = './logs/%s_%02d.log' % (str(date.today()), hour)
    file_handler = logging.FileHandler(filename)
    file_handler.setLevel(logging.INFO)
    'Sets the format for the logfile'
    formatter = logging.Formatter("%(asctime)s %(filename)s, %(lineno)d, %(funcName)s: %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    'Gets the elastic search logger'
    logging.getLogger('elasticsearch').setLevel(logging.INFO)
    
    logging.info('Started run')

    data = Data()
    data.getDataFromSource()
    data.saveData()
    data.sortByStandard()
    data.saveGraph()

    gc.collect()

    'Increment the hour by one'
    hour = hour + 1
    if hour >= 24:
        hour = 0

    logging.info('Finished')

