from data import Data
from datetime import datetime, date
import sys

done = False

ctime = datetime.now()
stype = ''
'Repeats until a valid date is selected'
while not done:
    try:
        'Gets the user to select a record'
        print 'Select year:'
        year = int(raw_input())
        
        print 'Select month:'
        month = int(raw_input())
        
        print 'Select day:'
        day = int(raw_input())
        
        print 'Select hour:'
        hour = int(raw_input())
        
        'Gets the type of graph the user wants'
        print 'top 10 accesses(t), standard(s), all(a), top 10 files(f),'
        print 'bad distribution small(bs), bad distribution large(bl)'
        stype = raw_input()
        
        ctime = datetime(year, month, day, hour, 0, 0)
        done = True
    except:
        print 'please enter a valid date'

data = Data()
data.getDataFromDatabase(ctime)

if stype == 's':
    data.sortByStandard()
if stype == 't':
    data.sortByTop10Accesses()
if stype == 'f':
    data.sortByTop10Files()
if stype == 'bs':
    data.sortByBad(True)
if stype == 'bl':
    data.sortByBad(False)

data.showGraph()
