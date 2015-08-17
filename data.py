import matplotlib as mpl
'''Forces matplotlib to use the Qt4Agg backend whick can create graphs that
    can be shown in another window.
    Needs to be run before any other matplotlib modules import'''
mpl.use('Qt4Agg')
from mpl_toolkits.mplot3d import Axes3D
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.pyplot as plt
import numpy as np
import sys
import castor_tools
from time import sleep
from datetime import datetime, date
import elasticsearch
import threading
import Queue
import MySQLdb
import math
import logging
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import json

class Data:
    """Can get data from source or the mysql database,
        then draw it on a matplotlib 3d graph,
        either showing it or saving as a png file"""
    
    def __init__(self):
        '''All need to be private variables,
            so other instances do not interfere with them'''
        self.datasets = []
        self.diskServers = []
        self.noOfFiles = []
        self.totalInDataset = []
        self.diskServersInDataset = []
        'Gets the old disk servers and datasets from the mysql database'
        db = MySQLdb.connect(db='logs')
        'Gets a cursor to use with the database'
        cursor = db.cursor()
        
        'Quiery to get all the disk servers'
        cursor.execute("select name from diskServers")
        dset = cursor.fetchall()
        for i in dset:
            self.diskServers.append(i[0])
            'Sorts the disk servers into alphabetical order'
            self.diskServers.sort()
            'Adds another row to each of the noOfFiles'
            for x in range(len(self.datasets)):
                self.noOfFiles[x].append(0)

        'Quiery to get all the datasets'
        cursor.execute("select name from datasets")
        dset = cursor.fetchall()
        for i in dset:
            self.datasets.append(i[0])
            'Sorts the datasets into alphabetical order'
            self.datasets.sort()
            'Sets up the arrays for use'
            self.noOfFiles.append([])
            self.diskServersInDataset.append(0)
            for j in range(len(self.diskServers)):
                self.noOfFiles[len(self.datasets) - 1].append(0)
            self.totalInDataset.append(0)

        'Closes the connection to the database'
        db.close()

        self.accesses = []
        self.accessesInDataset = []
        for i in range(len(self.datasets)):
            self.accesses.append([])
            self.accessesInDataset.append(0)
            for j in range(len(self.diskServers)):
                self.accesses[i].append(0)


    def getDataFromSource(self):
        """Gets the data from source,
            and makes it ready to use"""

        self._getFromCastor()

        for i in range(len(self.diskServers)):
            '''Removes the end of the disk servers,
                so they are the same as got from elasticSearch'''
            self.diskServers[i] = self.diskServers[i].replace('.gridpp.rl.ac.uk', '')

        self._getFromElasticsearch()

        for i in range(len(self.diskServers)):
            'Readd the end part'
            self.diskServers[i] = self.diskServers[i] + '.gridpp.rl.ac.uk'
        

    def _getFromCastor(self):
        """Gets the data from castor"""
        try:
            db = MySQLdb.connect(db='logs')
            'Gets a cursor to use with the database'
            cursor = db.cursor()
            
            'connect to stager'
            stconn = castor_tools.connectToStager()
            stcur = stconn.cursor()

            'The statment to search with'
            sqlStatement = '''
                select dataset, name, count(*)
                from ( select regexp_replace(substr(cf.lastknownfilename,1,instr(cf.lastknownfilename, '/', -1, 2)-1 ), '/castor/ads.rl.ac.uk/prod/cms/disk/store')
                    dataset, ds.name name
                    from castorfile cf, diskcopy dc, filesystem fs, diskserver ds
                    where
                        dc.castorfile = cf.id and
                        dc.filesystem = fs.id and
                        fs.diskserver = ds.id and
                        not regexp_like(cf.LASTKNOWNFILENAME, 'test', 'i') and
                        not REGEXP_LIKE(cf.LASTKNOWNFILENAME, 'backfill', 'i') and
                        REGEXP_LIKE(cf.lastknownfilename, 'disk')

                    order by ds.name, dataset)
                group by dataset, name
                '''
            'Runs the statment'
            stcur.execute(sqlStatement)
            'Gets all the results'
            rows = stcur.fetchall()

            for line in rows:
                if line[0] in self.datasets and line[1] in self.diskServers:
                    'Updates the arrays'
                    self.totalInDataset[self.datasets.index(line[0])] = self.totalInDataset[self.datasets.index(line[0])] + int(line[2])
                    self.diskServersInDataset[self.datasets.index(line[0])] = self.diskServersInDataset[self.datasets.index(line[0])] + 1
                else:
                    if line[1] not in self.diskServers:
                        self.diskServers.append(line[1])
                        self.diskServers.sort()
                        i = self.diskServers.index(line[1])
                        'Add newparts to the arrarys, in the right place'
                        for x in range(len(datasets)):
                            self.noOfFiles[x].insert(i, 0)
                            self.accesses.insert(i, 0)
                        'Inserts the new disk server into mysql'
                        cursor.execute(
                            "insert into diskServers (name) values (%s)", line[1])
                    if line[0] not in self.datasets:
                        self.datasets.append(line[0])
                        self.datasets.sort()
                        i = self.datasets.index(line[0])
                        'Add newparts to the arrarys, in the right place'
                        self.noOfFiles.insert(i, [])
                        self.diskServersInDataset.insert(i, 1)
                        self.accessesInDataset.insert(i, 0)
                        self.accesses.insert(i, [])
                        for j in range(len(self.diskServers)):
                            self.noOfFiles[i].append(0)
                            self.accesses[i].append(0)
                        self.totalInDataset.insert(i, int(line[2]))
                        'Inserts the new dataset into mysql'
                        cursor.execute(
                            "insert into datasets (name) values (%s)", line[0])
                'adds the number of files to the right position'
                self.noOfFiles[self.datasets.index(
                    line[0])][self.diskServers.index(line[1])] = int(line[2])

            try:
                'Disconnect from the database'
                castor_tools.disconnectDB(stconn)
                'Closes the connection to the database'
                db.close()
            except Exception:
                pass
        except Exception, e:
            'Logs any exeptions'
            logging.exception(e)
            'sends the error to the next try except loop'
            pass
            

    def _getFromElasticsearch(self):
        """Gets the accesses data from elastic search"""
        
        count = 0
        actual = 0
        try:
            'Creates a connection to elastic search'
            self.es = elasticsearch.Elasticsearch('elasticsearch1.gridpp.rl.ac.uk')

            'Creates a scroll quiery, that stays in the servers memory 5 minutes'
            res = self.es.search(size = 50, scroll='5m', search_type = 'scan', body={
                "fields": ["castor_NSFILEID", "castor_Filename"],
                "query": {
                    "filtered": {
                        "query": {
                            "match": {"castor_Type": "StagePrepareToGetRequest"}
                            },
                        "filter": {
                            "and": [
                            {
                                "term": {"@source_host": "lcgcstg02"}
                                },
                            {
                                "term": {"syslog_program": "stagerd"}
                                },
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": "now-1h",
                                        "lte": "now"
                                        }
                                    }
                                }
                            ]}
                            }}})
            
            sid = res['_scroll_id']
            scrollsize = res['hits']['total']
            logging.info('Total hits: ' + str(res['hits']['total']))

            'Creates a queue for a threads to put data into'
            myQueue = Queue.Queue()
            'sets a name idenifier so it can be told when all threads are finished'
            threadTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            'While there are still more hits'
            while scrollsize > 0:
                '''Scrolls elastic search to get more results.
                    The first scroll, created above has no results in it.
                    After this call the scroll stays in the servers
                    memory for another 5 minutes'''
                res = self.es.scroll(scroll_id = sid, scroll = '5m')
                'The scroll id is needed to get the next scroll'
                sid = res['_scroll_id']
                'Update scrollsize with the size of the latest batch'
                scrollsize = len(res['hits']['hits'])
                count = count + scrollsize
                data = res['hits']['hits']
                
                '''Makes sure the thread count does not get above 30,
                    as this causes timeouts from elastic search'''
                while threading.activeCount() > 30:
                    'Waits until a thread finishes'
                    sleep(1)

                'Creates a new thread. with the data from the last scroll'
                thr = threading.Thread(target = self._search, args = (data, myQueue))
                thr.name = threadTime
                'Starts the thread'
                thr.start()

            'Makes sure the program does not continue while threads are running'
            stillRunning = True
            while stillRunning:
                while not myQueue.empty():
                    'Gets the accesses from the queue'
                    val = myQueue.get()
                    self.accesses[val[0]][val[1]] = self.accesses[val[0]][val[1]] + 1
                    actual = actual + 1
                    self.accessesInDataset[val[0]] = self.accessesInDataset[val[0]] + 1
                stillRunning = False
                for thread in threading.enumerate():
                    'If one of the threads is running it will have the time name'
                    if thread.name == threadTime:
                        stillRunning = True
                
            logging.info('Accesses: ' + str(count) + ', found disk servers: ' + str(actual))
            
        except Exception as e:
            'Log any exeptions from elastic search, but ignore them'
            logging.exception(e)
            logging.error(str(sys.exc_info()))
            

    def _getDiskServerForId(self, data_id):
        """Finds which disk server a certian file is in"""
        res1 = self.es.search(body={
            "fields": ["@source_host"],
            "query": {
                "filtered": {
                    "query": {
                        "match": {"castor_NSFILEID": data_id}
                        },
                    "filter": {
                        "or": [
                            {
                                "term": {"syslog_program": "diskmanagerd"}
                                },
                            {
                                "term": {"syslog_program": "stagerjob"}
                                }]
                        }
                }}})
        
        for i1 in res1['hits']['hits']:
            if 'gdss' in i1['fields']['@source_host'][0]:
                '''Checks to see if result starts with gdss,
                    which indicates diskserver name.
                    Returns the disk server if true'''
                return i1['fields']['@source_host'][0]

    def _search(self, data, myQueue):
        """Takes the data from a scroll,
            gets which are actual entries,
            and finds their disk server.
            
            Puts the disk server number and
            the dataset number in the queue."""
        for i in data:
            'sets j to the actual data'
            j = i['fields']

            'The data comes back in array form, so the [0] is needed'
            filename = j['castor_Filename'][0]
            for s in self.datasets:
                'Determines whether the filename is in datasets'
                if s in filename:
                    data = self.datasets.index(s)
                    disksr = ''
                    'Checks to make sure the is a file id'
                    if 'castor_NSFILEID' in j:
                        'Gets the disk server for the file id'
                        disksr = self._getDiskServerForId(j['castor_NSFILEID'][0])
                    if disksr:
                        disk = self.diskServers.index(disksr)
                        'Adds the accesses to the queue'
                        myQueue.put((data, disk))
                            
    def getDataFromDatabase(self, time):
        """Gets the data from a mysql database,
            with the timestamp time.

            time must be a datetime that is in the mysql database"""

        self.time = time

        db = MySQLdb.connect(db='logs')
        'Gets a cursor to use with the database'
        cursor = db.cursor()

        cursor.execute("""select datasets.name, diskServers.name, files.accesses,
               files.noOfFiles from files
               join (diskServers, datasets) on (files.dataset = datasets.id and
               files.diskServer = diskServers.id)
               where files.timestamp = %s
               """, time.strftime('%Y-%m-%d %H:%M:%S'))

        for line in cursor.fetchall():
            'Adds the data to the relevant array'
            self.noOfFiles[self.datasets.index(line[0])][self.diskServers.index(line[1])] = int(line[3])
            self.totalInDataset[self.datasets.index(line[0])] = self.totalInDataset[self.datasets.index(line[0])] + int(line[3])
            self.diskServersInDataset[self.datasets.index(line[0])] = self.diskServersInDataset[self.datasets.index(line[0])] + 1
            self.accesses[self.datasets.index(line[0])][self.diskServers.index(line[1])] = int(line[2])
            self.accessesInDataset[self.datasets.index(line[0])] = self.accessesInDataset[self.datasets.index(line[0])] + int(line[2])

        'Closes the connection to the database'
        db.close()

    def saveData(self):
        """Saves the data into the mysql database"""

        'Gets the old disk servers and datasets from the mysql database'
        db = MySQLdb.connect(db='logs')
        'Gets a cursor to use with the database'
        cursor = db.cursor()

        'Save to the mysql database'
        for i in range(len(self.datasets)):
            for j in range(0, len(self.diskServers)):
                'Only save if there are files to save'
                if self.noOfFiles[i][j] > 0:
                    cursor.execute("select id from datasets where name=%s", self.datasets[i])
                    datasetk = cursor.fetchall()[0][0]
                    
                    name = self.diskServers[j]
                    cursor.execute("select id from diskServers where name=%s", name)
                    diskServerk = cursor.fetchall()[0][0]
                    
                    ctime = datetime.now()
                    'Makes a time that the record was got at'
                    ctime = datetime(ctime.year, ctime.month, ctime.day, datetime.now().hour, 0, 0)
                    
                    'adds the record to the database'
                    cursor.execute("""insert into files (noOfFiles,
                                        accesses, dataset, diskServer, timestamp)
                                        values (%s, %s, %s, %s, %s)""",
                                   [self.noOfFiles[i][j], self.accesses[i][j],
                                    datasetk, diskServerk,
                                    ctime.strftime('%Y-%m-%d %H:%M:%S')])
        
        'Closes the connection to the database'
        db.close()

    def sortByStandard(self):
        """Sorts the graph so that all the low data is got rid of"""

        for i in range(len(self.datasets)-1, -1, -1):
            'Gets rid of any dataset with less than 6 files'
            if self.totalInDataset[i] < 6:
                self.totalInDataset.pop(i)
                self.datasets.pop(i)
                self.noOfFiles.pop(i)
                self.diskServersInDataset.pop(i)
                self.accesses.pop(i)
                self.accessesInDataset.pop(i)

    def sortByTop10Accesses(self):
        """Sorts the graph so only the top 10 accesses
            for a whole dataset remain in the data"""

        highest10 = []
        lowest = sys.maxint
        lowestloc = 0
        for i in range(len(self.datasets)):
            'If not done 10 datasets, add another to fill it up'
            if len(highest10) < 10:
                highest10.append(self.datasets[i])
                'Check if the one being added is the lowest'
                if self.accessesInDataset[i] < lowest:
                    lowest = self.accessesInDataset[i]
                    lowestloc = i
            elif self.accessesInDataset[i] > lowest:
                'Check to see if current dataset has more accesses than the lowest'
                highest10[lowestloc] = self.datasets[i]
                lowest = self.accessesInDataset[i]
                'Find the new lowest'
                for j in range(len(highest10)):
                    if self.accessesInDataset[self.datasets.index(highest10[j])] < lowest:
                        lowest = self.accessesInDataset[self.datasets.index(highest10[j])]
                        lowestloc = j
                
        for i in range(len(self.datasets)-1, -1, -1):
            'Remove any datasets that are not in the top 10'
            if not self.datasets[i] in highest10:
                self.totalInDataset.pop(i)
                self.datasets.pop(i)
                self.noOfFiles.pop(i)
                self.diskServersInDataset.pop(i)
                self.accesses.pop(i)
                self.accessesInDataset.pop(i)
        'Print the highest 10, and how many accesses they have'
        for i in highest10:
            print i, self.accessesInDataset[self.datasets.index(i)]

    def sortByTop10Files(self):
        """Sorts the graph so only the top 10 files
            for a whole dataset remain in the data"""

        highest10 = []
        lowest = sys.maxint
        lowestloc = 0
        for i in range(len(self.datasets)):
            'If not done 10 datasets, add another to fill it up'
            if len(highest10) < 10:
                highest10.append(self.datasets[i])
                'Check if the one being added is the lowest'
                if self.totalInDataset[i] < lowest:
                    lowest = self.totalInDataset[i]
                    lowestloc = i
            elif self.totalInDataset[i] > lowest:
                'Check to see if current dataset has more accesses than the lowest'
                highest10[lowestloc] = self.datasets[i]
                lowest = self.totalInDataset[i]
                'Find the new lowest'
                for j in range(len(highest10)):
                    if self.totalInDataset[self.datasets.index(highest10[j])] < lowest:
                        lowest = self.totalInDataset[self.datasets.index(highest10[j])]
                        lowestloc = j
                
        for i in range(len(self.datasets)-1, -1, -1):
            'Remove any datasets that are not in the top 10'
            if not self.datasets[i] in highest10:
                self.totalInDataset.pop(i)
                self.datasets.pop(i)
                self.noOfFiles.pop(i)
                self.diskServersInDataset.pop(i)
                self.accesses.pop(i)
                self.accessesInDataset.pop(i)
                
        'Print the highest 10, and how many files they have'
        for i in highest10:
            print i, self.totalInDataset[self.datasets.index(i)]

    def sortByBad(self, small):
        """Selects any dataset where the distribution of files across the diskservers is bad

            Small is a bool that indicates whether small file numbers are wanted"""

        highest10 = []
        lowest = sys.maxint
        lowestloc = 0

        for i in range(len(self.datasets)-1, -1, -1):
            if self.totalInDataset[i] <= len(self.diskServers) + 1:
                if small:
                    amount = 0
                    for j in range(len(self.diskServers)):
                        if self.noOfFiles[i][j] - (float(self.totalInDataset[i])/float(len(self.diskServers))) > amount:
                            amount = self.noOfFiles[i][j] - (float(self.totalInDataset[i])/float(len(self.diskServers)))

                    if len(highest10) < 10:
                        highest10.append(self.datasets[i])
                        'Check if the one being added is the lowest'
                        if amount < lowest:
                            lowest = amount
                            lowestloc = len(highest10)-1
                    elif amount > lowest:
                        'Check to see if current dataset has more accesses than the lowest'
                        highest10[lowestloc] = self.datasets[i]
                        lowest = amount
                        'Find the new lowest'
                        for x in range(len(highest10)):
                            for j in range(len(self.diskServers)):
                                if self.noOfFiles[self.datasets.index(highest10[x])][j] - (float(self.totalInDataset[self.datasets.index(highest10[x])])/float(len(self.diskServers))) > amount:
                                    amount = self.noOfFiles[self.datasets.index(highest10[x])][j] - (float(self.totalInDataset[self.datasets.index(highest10[x])])/float(len(self.diskServers)))
                            if amount < lowest:
                                lowest = amount
                                lowestloc = x

                else:
                    self.totalInDataset.pop(i)
                    self.datasets.pop(i)
                    self.noOfFiles.pop(i)
                    self.diskServersInDataset.pop(i)
                    self.accesses.pop(i)
                    self.accessesInDataset.pop(i)
            else:
                if small:
                    self.totalInDataset.pop(i)
                    self.datasets.pop(i)
                    self.noOfFiles.pop(i)
                    self.diskServersInDataset.pop(i)
                    self.accesses.pop(i)
                    self.accessesInDataset.pop(i)
                else:
                    amount = 0
                    for j in range(len(self.diskServers)):
                        if float(self.noOfFiles[i][j] - (float(self.totalInDataset[i])/float(len(self.diskServers))))/float(self.totalInDataset[i]) > amount:
                            amount = float(self.noOfFiles[i][j] - (float(self.totalInDataset[i])/float(len(self.diskServers))))/float(self.totalInDataset[i])

                    if len(highest10) < 10:
                        highest10.append(self.datasets[i])
                        'Check if the one being added is the lowest'
                        if amount < lowest:
                            lowest = amount
                            lowestloc = len(highest10)-1
                    elif amount > lowest:
                        'Check to see if current dataset has more accesses than the lowest'
                        highest10[lowestloc] = self.datasets[i]
                        lowest = amount
                        'Find the new lowest'
                        for x in range(len(highest10)):
                            amount = 0
                            for j in range(len(self.diskServers)):
                                if float(self.noOfFiles[self.datasets.index(highest10[x])][j] - (float(self.totalInDataset[self.datasets.index(highest10[x])])/float(len(self.diskServers))))/float(self.totalInDataset[self.datasets.index(highest10[x])]) > amount:
                                    amount = float(self.noOfFiles[self.datasets.index(highest10[x])][j] - (float(self.totalInDataset[self.datasets.index(highest10[x])])/float(len(self.diskServers))))/float(self.totalInDataset[self.datasets.index(highest10[x])])
                            if amount < lowest:
                                lowest = amount
                                lowestloc = x
        for i in range(len(self.datasets)-1, -1, -1):
            'Remove any datasets that are not in the top 10'
            if not self.datasets[i] in highest10:
                self.totalInDataset.pop(i)
                self.datasets.pop(i)
                self.noOfFiles.pop(i)
                self.diskServersInDataset.pop(i)
                self.accesses.pop(i)
                self.accessesInDataset.pop(i)
        for i in highest10:
            print i
        
        with open('data.txt', 'w') as outfile:
            dicton = {}
            for i in range(len(highest10)):
                hlist = []
                for j in range(len(self.diskServers)):
                    hlist.append((self.diskServers[j], self.noOfFiles[i][j]))
                hlist.sort(key=lambda x: x[1], reverse=True)
                dicton[highest10[i]] = hlist
            json.dump(dicton, outfile)

    def printInfo(self, x, y):
        """Prints a read out for a given record, as given by x and y"""
        if not self.accesses[x][y] == 0:
            amount = float(self.accesses[x][y]) / float(self.accessesInDataset[x])
        else:
            amount = 0

        print "Disk server: '" + self.diskServers[y] + "' in dataset: '" + self.datasets[x] + "'"
        print 'has ' + str(self.accesses[x][y]) + ' out of ' + str(self.accessesInDataset[x]) + ' accesses (' + str((amount)*100) + '%)'
        print 'and ' + str(self.noOfFiles[x][y]) + ' out of ' + str(self.totalInDataset[x]) + ' files'
        print 'There are ' + str(self.diskServersInDataset[x]) + ' disk server(s) in the dataset'

    def saveGraph(self):
        """Saves the data as a picture,
            after creating it"""

        colors = []
        'Creates the color array'
        for i in range(len(self.datasets)):
            colors.append([])
            for j in range(0, len(self.diskServers)):
                'Checks if there will be a divide by 0 error'
                if not self.accessesInDataset[i] == 0:
                    '''Divides the accesses by the number in its dataset,
                        to get the amount'''
                    amount = float(self.accesses[i][j]) / float(self.accessesInDataset[i])
                else:
                    'else sets the amount to 0'
                    amount = 0
                
                if self.noOfFiles[i][j] == 0  and not(i==0 and j == 0):
                    '''If the noOfFiles is 0, uses green as the color,
                        due to the colors not diplaying if all rgb values.
                        The green color cannot be the first in the array,
                        as this also causes an error'''
                    colors[i].append('g')
                else:
                    if not amount == 0:
                        'Calculates a log value for the amount'
                        amount = (math.log10(amount) / 4) + 1
                    colors[i].append([1.0, 1.0 - amount, 1.0 - amount])
            
        my_dpi = 100
        '''Create the size of the image.
            This is for ffmpeg (see animation.py) as it requires images
            with even heights and widths.

            As the backend is being used directly,
            a figure and a canvas is needed'''
        fig = Figure(figsize=(8, 6), dpi=my_dpi)
        canvas = FigureCanvas(fig)

        'matplotlib requires numpy arrays'
        data_array = np.array(self.noOfFiles)
        colors = np.array(colors)
        
        'Add a 3d subplot to the image'
        ax = fig.add_subplot(111, projection='3d')

        for i in range(len(self.diskServers)):
            'Gets rid of the start and end, so they will fit on the graph'
            self.diskServers[i] = self.diskServers[i].replace('gdss', '')
            self.diskServers[i] = self.diskServers[i].replace('.gridpp.rl.ac.uk', '')

        'Sets the number of ticks on the x axis, the their names'
        ax.set_xticks(range(len(self.diskServers)))
        ax.set_xticklabels(self.diskServers, fontsize=6, rotation='vertical')

        'Adds the date to the graph'
        ax.text2D(0.05, 0.95, "Date: " + str(date.today()) + ' ' + str(datetime.now().hour) + ':00', transform=ax.transAxes)
        
        'Set the axes labels'
        ax.set_xlabel('Disk server')
        ax.set_ylabel('Datasets')
        ax.set_zlabel('Count')
        
        'Create two arrays for the x and y positions of the bar'
        xpos, ypos = np.meshgrid( np.arange(data_array.shape[1]),
                                  np.arange(data_array.shape[0]) )

        'matplotlib also requires flattened arrays'
        xpos = xpos.flatten()
        ypos = ypos.flatten()

        colors = colors.flatten()
        dz = data_array.flatten()
        zpos = np.zeros(len(dz))
        'set the bar width and height to 0.2'
        dx = 0.2 * np.ones_like(zpos)
        dy = dx.copy()

        'create the histogram'
        ax.bar3d(xpos, ypos, zpos, dx, dy, dz, colors)

        'create a color map for the sidebar'
        cdict2 = {'red':   ((0.0, 1.0, 1.0),
                           (1.0, 1.0, 1.0)),

                 'green': ((0.0, 1.0, 1.0),
                           (1.0, 0.0, 0.0)),

                 'blue':  ((0.0, 1.0, 1.0),
                           (1.0, 0.0, 0.0))
                }
        
        red2 = LinearSegmentedColormap('Red2', cdict2)
        'Register the colormap'
        plt.register_cmap(cmap=red2)

        norm = mpl.colors.Normalize(vmin=0, vmax=100)
        'Get the colormap back again'
        cmap = plt.get_cmap('Red2')

        'Create some axes for the sidebar'
        cax = fig.add_axes([0.905, 0.2, 0.02, 0.6])
        'Create the colorbar'
        cb = mpl.colorbar.ColorbarBase(cax, cmap=cmap, norm=norm, spacing='proportional')
        cb.set_label('% of accesses to diskserver in dataset')

        'Set where the ticks are'
        cb.set_ticks([0, 25, 50, 75, 100])
        'Set the ticks on the colorbar'
        cb.set_ticklabels(['0', '0.1', '1', '10', '100'])
        'Update what the ticks show'
        cb.update_ticks()

        'Save the image'
        canvas.print_figure('images/graph_%s_%02d.png' % (str(date.today()), datetime.now().hour), bbox_inches='tight')

        '''This close the picture.
            Needed so the program does not use all the computers memory'''
        fig.clf()
        plt.close()

    def showGraph(self):
        """Displays the data on a graph"""

        colors = []
        highPercentCount = 0
        'Creates the color array'
        for i in range(len(self.datasets)):
            colors.append([])
            for j in range(0, len(self.diskServers)):
                'Checks if there will be a divide by 0 error'
                if not self.accesses[i][j] == 0:
                    '''Divides the accesses by the number in its dataset,
                        to get the amount'''
                    amount = float(self.accesses[i][j]) / float(self.accessesInDataset[i])
                else:
                    amount = 0
                
                if ((amount > 0.5) and (self.noOfFiles[i][j] > 1)):
                    'If above 50% accesses, print the record'
                    self.printInfo(i, j)
                    highPercentCount = highPercentCount + 1
                
                if self.noOfFiles[i][j] == 0  and not(i==0 and j == 0):
                    '''If the noOfFiles is 0, uses green as the color,
                        due to the colors not diplaying if all rgb values.
                        The green color cannot be the first in the array,
                        as this also causes an error'''
                    colors[i].append('g')
                else:
                    if not amount == 0:
                        'Calculates a log value for the amount'
                        amount = (math.log10(amount) / 4) + 1
                    colors[i].append([1.0, 1.0 - amount, 1.0 - amount])

        'Print the total above 50% of accesses'
        print 'Total: ' + str(highPercentCount)

        'matplotlib requires numpy arrays'
        data_array = np.array(self.noOfFiles)
        colors = np.array(colors)

        my_dpi = 100
        'Create the size of the image.'
        fig = plt.figure(
            figsize=(8,6), dpi=my_dpi)
        'Add a 3d subplot to the image'
        ax = fig.add_subplot(111, projection='3d')

        '''Uncomment to get disk server names and
            accesses in dataset on the x and y axis.
            It does not work with the onclick mechanism'''
        """
        for i in range(len(diskServers)):
            'Cut down the disk server names so they can be displayed'
            diskServers[i] = diskServers[i].replace('gdss', '')
            diskServers[i] = diskServers[i].replace('.gridpp.rl.ac.uk', '')
        plt.xticks(range(len(diskServers)), diskServers, fontsize=6, rotation='vertical')
        plt.yticks(range(len(datasets)), accessesInDataset, fontsize=6, rotation='horizontal')
        """

        'Adds the date to the graph, using the current time if not read from the database'
        if self.time:
            ax.text2D(0.05, 0.95, "Date: " + self.time.strftime('%Y-%m-%d %H:%M:%S'), transform=ax.transAxes)
        else:
            ax.text2D(0.05, 0.95, "Date: " + str(date.today()) + ' ' + str(datetime.now().hour) + ':00', transform=ax.transAxes)

        'Set the axes labels'
        ax.set_xlabel('Disk server')
        ax.set_ylabel('Dataset')
        ax.set_zlabel('Count')

        'Create two arrays for the x and y positions of the bar'
        xpos, ypos = np.meshgrid( np.arange(data_array.shape[1]),
                                  np.arange(data_array.shape[0]) )

        'matplotlib also requires flattened arrays'
        xpos = xpos.flatten()
        ypos = ypos.flatten()

        colors = colors.flatten()
        dz = data_array.flatten()
        zpos = np.zeros(len(dz))
        'set the bar width and height to 0.2'
        dx = 0.2 * np.ones_like(zpos)
        dy = dx.copy()

        'create the histogram'
        ax.bar3d(xpos, ypos, zpos, dx, dy, dz, colors)

        'create a color map for the sidebar'
        cdict2 = {'red':   ((0.0, 1.0, 1.0),
                           (1.0, 1.0, 1.0)),

                 'green': ((0.0, 1.0, 1.0),
                           (1.0, 0.0, 0.0)),

                 'blue':  ((0.0, 1.0, 1.0),
                           (1.0, 0.0, 0.0))
                }

        blue_red2 = LinearSegmentedColormap('Red2', cdict2)
        'Register the colormap'
        plt.register_cmap(cmap=blue_red2)

        norm = mpl.colors.Normalize(vmin=0, vmax=100)
        'Get the colormap back again'
        cmap = plt.get_cmap('Red2')

        'Create some axes for the sidebar'
        cax = fig.add_axes([0.905, 0.2, 0.02, 0.6])
        'Create the colorbar'
        cb = mpl.colorbar.ColorbarBase(cax, cmap=cmap, norm=norm, spacing='proportional')
        cb.set_label('% of accesses to diskserver in dataset')

        'Set where the ticks are'
        cb.set_ticks([0, 25, 50, 75, 100])
        'Set the ticks on the colorbar'
        cb.set_ticklabels(['0', '0.1', '1', '10', '100'])
        'Update what the ticks show'
        cb.update_ticks()

        def onclick(event):
            """Takes the 2d coordinates from the event,
                converts them to 3d coordinates,
                then finds which records shown are near the 3d coordinates.

                This function cannot be used with custom ticks"""

            'Gets the 3d coordinates. Returns a string'
            coords = ax.format_coord(event.xdata, event.ydata)

            'Splits the coordinates into x, y and z parts'
            x, y, z = coords.split(',')

            'Gets the float value out of the x string'
            x = x.replace('x=', '')
            x = x.replace(' ', '')
            x = float(x)

            'Gets the float value out of the y string'
            y = y.replace('y=', '')
            y = y.replace(' ', '')
            y = float(y)

            'Gets the float value out of the z string'
            z = z.replace('z=', '')
            z = z.replace(' ', '')
            z = float(z)

            'Rounds the x value to the nearest whole number'
            x =  int(round(x, 0))
            'Makes sure the x value is in the range of data'
            if x > len(self.diskServers) - 1:
                x = len(self.diskServers) - 1
            if x < 0:
                x = 0

            'Rounds the y value to the nearest whole number'
            y =  int(round(y, 0))
            'Makes sure the y value is in the range of data'
            if y > len(self.datasets) - 1:
                y = len(self.datasets) - 1
            if y < 0:
                y = 0

            'Finds any record that is near the point'
            for i in range(len(self.datasets)):
                if i < y + 50 and i > y - 50:
                    for j in range(len(self.diskServers)):
                        if self.noOfFiles[i][j] < z + 50 and self.noOfFiles[i][j] > z - 50 and j < x + 1 and j > x - 1 and not self.noOfFiles[i][j] == 0:
                            self.printInfo(i, j)
                    
            'Prints the coordinates selected'
            print 'Selected: (' + str(x) + ', ' + str(y) + ', ' + str(z) + ')'
        
        'Sets the function for a click event'
        cid = fig.canvas.mpl_connect('button_press_event', onclick)

        plt.show()


