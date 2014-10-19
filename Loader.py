# -*- coding: utf-8 -*-
"""
Created on Sat Oct 18 19:00:43 2014

@author: des
"""

import csv
import json
from pymongo import MongoClient 

readings = ["123,01-10-2010,00:00,0.0023",
            "123,01-10-2010,00:30,0.0023",
            "123,01-10-2010,01:00,0.0023",
            "123,02-10-2010,00:00,0.0023",
            "123,02-10-2010,00:30,0.0023",
            "123,02-10-2010,01:00,0.0023",
            "123,03-10-2010,00:00,0.0023",
            "124,03-10-2010,00:30,0.0023",
            "124,03-10-2010,01:00,0.0023",            
            "123,04-09-2010,01:00,0.0099"]


#data = {}

#==============================================================================
# def makeDict(start, entries, content, keyprefix):
#     dic = {keyprefix + str(x):content for x in range(start,entries + start)}
#     dic['cons'] = 0
#     return dic
#==============================================================================

#def makeStructure():
#    dy = makeDict(0,48,0,"period") #Structure for a single day'd readings intialised to 0
#    mn = makeDict(1,31,dy,"day") #structure for a months reading containing the day  
#    yr = makeDict(1,12,mn, "month") #structure for a year's readings conatining months 
#    return yr

#==============================================================================
# def addRec(line):
#     custId, readDate, readTime, reading = line.split(",")
#     reading = float(reading)
#     readDate = readDate.split("-")
#     readDay, readMonth, readYear = readDate
#     readMonth = "month" + str(int(readMonth))
#     readDay = "day" + str(int(readDay))
#     readHour, readMin = readTime.split(":")
#     readHour = int(readHour)
#     readMin = int(readMin)
#     readPeriod = readHour * 2 + readMin / 30   
#     readPeriod = "period" + str(readPeriod)    
#     #look to find if a dictionary entry exists for the id
#     #if it doesn't then create it and add an entry for the year
#     #with prepoulated month, day and time segment entries
#     # that can just be updated directly with no more messing...
#     if not custId in data:
#         data[custId] = {}
#     if not readYear in data[custId]:
#         data[custId][readYear] = makeStructure()
#         
#     data[custId][readYear]['cons'] += reading
#     data[custId][readYear][readMonth]['cons'] += reading
#     data[custId][readYear][readMonth][readDay]['cons'] += reading
#     data[custId][readYear][readMonth][readDay][readPeriod] = reading
#==============================================================================
    
def addRec1(line, data):
    custId, readDate, readTime, reading = line.split(",")

    reading = float(reading)
    readDate = readDate.split("-")
    readDay, readMonth, readYear = readDate

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
    
def readCSV():
    curCustId = ""
    for row in readings:
        print row
        custId = row.split(",")[0]
        print custId    
        if custId != curCustId:
            # Save last customer
            if curCustId != "":
                collReadings.insert(data)
            data = {"_id": custId, 'cons': 0 }
            print data
            # Get the new one
            curCustId = custId
        #TODO     if curCustID <> custID then save the current record to the database
        #and retrieve the one for the new customer
        data = addRec1(row, data)
#Save the last update on exiting the loop
    collReadings.insert(data)    
    print data
    return data 
def openDatabase(db):
    client = MongoClient('localhost', 27017)    
    return client[db]
##print makeStructure()
def openCollection(collection, db):
    return db[collection]

collReadings = openCollection("Readings", openDatabase("Readings"))

data = readCSV()

f =  open("out.txt", "w")
json.dump(data, f)
f.close

#assert data[123][2010]['cons'] == 0.0069