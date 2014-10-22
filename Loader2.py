# -*- coding: utf-8 -*-
"""
Created on Tue Oct 21 18:59:52 2014

@author: des
"""

# -*- coding: utf-8 -*-
"""
Created on Sat Oct 18 19:00:43 2014

@author: des
"""

import csv
import json
from pymongo import MongoClient 


def addRec1(line, data):
    custId, readDate, readTime, reading = line

    reading = float(reading)
    readDate = readDate.split("-")
    readYear, readMonth, readDay = readDate

    readMonth = "month" + str(int(readMonth))
    readDay = "day" + str(int(readDay))
    readHour, readMin = readTime.split(":")
    readHour = int(readHour)
    readMin = int(readMin)
    readPeriod = readHour * 2 + readMin / 30   
    readPeriod = "period" + str(readPeriod)    
    #look to find if a dictionary entry exists for the id
    #if it doesn't then create it and add an entry for the year
    #with prepoulated month, day and time segment entries
    # that can just be updated directly with no more messing...

    #if not custId in data:
    #    data[custId] = {'cons': 0 }
    if not readYear in data:
        data[readYear] = {'cons': 0 }
    if not readMonth in data[readYear]:
        data[readYear][readMonth] = {'cons': 0 }
    if not readDay in data[readYear][readMonth]:
        data[readYear][readMonth][readDay] = {'cons': 0 }
        
    #TODO - if a period already exists then skip the update or remove the 
    # reading from the totals and replace????    
    print data    
    data['cons'] += reading
    data[readYear]['cons'] += reading
    data[readYear][readMonth]['cons'] += reading
    data[readYear][readMonth][readDay]['cons'] += reading
    data[readYear][readMonth][readDay][readPeriod] = reading
    return data
    
def addRec2(line, data):
    custId, readDate, readTime, reading = line

    reading = float(reading)
    readDate = readDate.split("-")
    readYear, readMonth, readDay = readDate
    #readYear = int(readYear)
    #readMonth = int(readMonth)
    #readDay =  int(readDay)
    readHour, readMin = readTime.split(":")
    readHour = int(readHour)
    readMin = int(readMin)
    readPeriod = readHour * 2 + readMin / 30   
    readPeriod = str(readPeriod    )
    #look to find if a dictionary entry exists for the id
    #if it doesn't then create it and add an entry for the year
    #with prepoulated month, day and time segment entries
    # that can just be updated directly with no more messing...

    #if not custId in data:
    #    data[custId] = {'cons': 0 }
#    if not readYear in data:
#        data[readYear] = {'cons': 0 }
#    if not readMonth in data[readYear]:
#        data[readYear][readMonth] = {'cons': 0 }
#    if not readDay in data[readYear][readMonth]:
#        data[readYear][readMonth][readDay] = {'cons': 0 }
        
    #TODO - if a period already exists then skip the update or remove the 
    # reading from the totals and replace????    
    #print     readYear
    #print data    
    #print data['readings']
    data['cons'] += reading
    yrIdx = next((i for i,d in enumerate(data['readings']) if d['year'] == readYear), None)
    if  yrIdx == None:
        data['readings'].append({'year': readYear, 'cons': 0, 'readings': []})
        yrIdx = len(data['readings']) - 1
    data['readings'][yrIdx]['cons'] += reading
    #print yrIdx
    #print data['readings'][0]
    mnIdx = next((i for i,d in enumerate(data['readings'][yrIdx]['readings']) if d['month'] == readMonth), None)
    if  mnIdx == None:
        data['readings'][yrIdx]['readings'].append({'month': readMonth, 'cons': 0, 'readings': []})
        mnIdx = len(data['readings']) - 1  
    data['readings'][yrIdx]['readings'][mnIdx]['cons'] += reading
    dyIdx = next((i for i,d in enumerate(data['readings'][yrIdx]['readings'][mnIdx]['readings']) if d['day'] == readDay), None)
    if  dyIdx == None:
        data['readings'][yrIdx]['readings'][mnIdx]['readings'].append({'day': readDay, 'cons': 0, 'readings': {}})
        dyIdx = len(data['readings']) - 1  
    data['readings'][yrIdx]['readings'][mnIdx]['readings'][dyIdx]['cons'] += reading    
    data['readings'][yrIdx]['readings'][mnIdx]['readings'][dyIdx]['readings'][readPeriod] =  reading    
    #data[readYear][readMonth][readDay]['cons'] += reading
    #data[readYear][readMonth][readDay][readPeriod] = reading
    return data

    
def readCSV(fname):
    with open(fname,"r") as inFile:
        readings = csv.reader(inFile, delimiter=",")
        
        curCustId = ""
        for row in readings:
            #print row
            custId = row[0]
            #print custId    
            if custId != curCustId:
                # Save last customer
                if curCustId != "":
          #          pass
                    writeResult = collReadings.update({'customer': curCustId},{'$set': data}, upsert = True)
                    print writeResult    
                   #get next one
                data = collReadings.find_one({'customer': custId})
         #       data = None    
                if data == None:
                    data = {"customer": custId, 'cons': 0, 'readings': [] }
                if '_id' in data:                
                    del data['_id']        
                #print data
                # Get the new one
                curCustId = custId
            #TODO     if curCustID <> custID then save the current record to the database
            #and retrieve the one for the new customer
            data = addRec2(row, data)
    #Save the last update on exiting the loop
        writeResult = collReadings.update({'customer': curCustId},{'$set': data}, upsert = True)
        print writeResult    
        
        return data 

def openDatabase(db):
    client = MongoClient('localhost', 27017)    
    return client[db]
##print makeStructure()
def openCollection(collection, db):
    return db[collection]

collReadings = openCollection("Readings", openDatabase("Readings"))

data = readCSV("C:\\SM\\POC Data File_1\\SAP-POC-DATA-1.CSV")

f =  open("out.txt", "w")
json.dump(data, f)
f.close

#assert data[123][2010]['cons'] == 0.0069
