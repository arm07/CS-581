import json
import requests
import Average
import datetime

import mysql.connector
from mysql.connector import errorcode
import numpy as np
import matplotlib.pyplot as plt
from scipy.cluster.vq import kmeans2, whiten


def kmeanscluster(starttime , endtime):
  global numpool
  try:
      cnn = mysql.connector.connect(
          user='root',
          password='qwerty',
          host='localhost',
          database='ride')
      # print("it works")
  except mysql.connector.Error as e:
      if e.errno == errorcode.ER_ACCESS_DENIED_CHANGE_USER_ERROR:
          print("something is wrong with username and password")
      elif e.errno == errorcode.ER_BAD_DB_ERROR:
          print("database doesn't exist")
      else:
          print(e)

cursor = cnn.cursor()

def selecteachpool():
      # the query is to select particular fields in the data and for filtering pool for setting thw Window pool sizes
      query = "Select medallion, dropoff_longitude, dropoff_latitude, passenger_count from jfk_data12_2013 WHERE pickup_datetime >= '" + str(starttime) + "'and pickup_datetime < '" + str(endtime) + "'"
      cursor.execute(query)
      result_set = cursor.fetchall()
      coord_list = []
      passenger_count = []
      for row in result_set:
          temp = [float(row[1]), float(row[2])]
          coord_list.append(temp)

      for row in result_set:
          passenger_count.append(int(row[3]))

      recordcount = 0
      for outerlist in coord_list:
          recordcount += 1
          # print (outerlist)
      return coord_list, passenger_count, recordcount

def getcoord_list():
      return coord_list

def getkvalue(recordcount):
      kvalue = (int)(recordcount / 4)
      # print("No of records:", count)
      return kvalue

def kmeanscluster(coord_list):
      coordinates = np.array(coord_list)
      return kmeans2(whiten(coordinates), kvalue, missing='warn',)

def plotkmeanscluster(coord_list, labels):
      coordinates = np.array(coord_list)
      plt.scatter(coordinates[:, 0], coordinates[:, 1], c=labels, iter = 5)
      plt.show()

def getclusterpoints(coord_list, labels, kvalue):
      clusterpt = [0 for x in range(kvalue)]
      for i in range(0, kvalue):
          clusterpt[i] = []
      listitem = 0
      for i in labels:
          clusterpt[i].append(coord_list[listitem])
          listitem = listitem + 1
      return clusterpt

def printclusterpoints(clusterpt, kvalue):   # To print the cluster of destination values
      print("\nkmeans clustering result \n")
      for i in range(0, kvalue):
          print("cluster number:", i, clusterpt[i])

def carassignment(clusterpt, kvalue):
      carassign = []
      carcount = -1
      for i in range(0, kvalue):
          for j in range(0, len(clusterpt[i])):
              if (j % 4 == 0):
                  carassign.append([])
                  carcount = carcount + 1
                  carassign[carcount].append(clusterpt[i][j])
              else:
                  carassign[carcount].append(clusterpt[i][j])
      return carassign, carcount + 1

def getcarassignment():
      return carassign, carcount



def getDistance(plat,plong,dlat,dlong):
      requestString = 'http://localhost:8989/route?point=' + str(plat) + '%2C' + str(plong) + '&point=' + str(
          dlat) + '%2C' + str(dlong) + '&vehicle=car'
      r = requests.get(requestString)

      res = json.loads(r.text)

      return_list = []
      if ('paths' in res):
          return_list.append(res['paths'][0]['distance'])
          return_list.append(res['paths'][0]['time'])
          return return_list
      else:
          return_list.append(-250)
          return_list.append(-250)
          return return_list

def getwithoutridesharingdistance(coord_list):
      global tnormaldist
      global wolength
      wolength = wolength + len(coord_list)
      normaldistance = 0
      dist = [0 for x in range(len(coord_list))]
      for i in range(0, len(coord_list)):
          # print (coord_list[i][0],coord_list[i][1])
          dist = getDistance(40.645574, -73.784866, coord_list[i][1], coord_list[i][0])
          normaldistance = normaldistance + dist[0]
          # print (dist[i])
      normaldistance = normaldistance/1000
      tnormaldist = tnormaldist + normaldistance
      print("\ndistance without ride sharing",normaldistance,"km")
      return normaldistance

def getwithridesharingdistance(carassign,carcount):
      global tridesharedist
      global wlength
      wlength = wlength + len(carassign)
      ridesharedist = 0
      for i in range(0,carcount):
          initlat = 40.645574
          initlog = -73.784866
          for j in range (0,len(carassign[i])):
              if(j!=0):
                  initlat = carassign[i][j-1][1]
                  initlog = carassign[i][j-1][0]
              # print(carassign[i][j],carassign[i][j])
              distance = getDistance(initlat, initlog, carassign[i][j][1], carassign[i][j][0])
              ridesharedist = ridesharedist + distance[0]

      ridesharedist = ridesharedist/1000
      tridesharedist = tridesharedist + ridesharedist
      print("\ndistance with ride sharing",ridesharedist,"km")
      return ridesharedist

def getsaveddistance(normaldistance,ridesharedist):
      global totaldistance
      distancesaved = (1-(ridesharedist/normaldistance))*100
      totaldistance = totaldistance + distancesaved
      print("\ndistance saved(percentage):",round(distancesaved,2),"%")
	  
	 
def printridesshared(carassign, carcount):
      print("\nRide shared cars and its destination\n")
      for i in range(0, carcount):
          print("car number:", i, carassign[i])
		  
coord_list,passenger_count,recordcount=selecteachpool()
		  
#if (len(coord_list) < 4):
#	return

	
kvalue = getkvalue(recordcount)
meanpoints, labels = kmeanscluster(coord_list)
# plotkmeanscluster(coord_list,labels)

clusterpt = getclusterpoints(coord_list, labels, kvalue)
# printclusterpoints(clusterpt, kvalue)
# print("kvalue:", kvalue)

carassign, carcount = carassignment(clusterpt, kvalue)
# printridesshared(carassign, carcount)
#
# print("\nTrips before ride sharing:", len(coord_list))
# print("\nTrips after ride sharing:", carcount)

cnn.commit()
cursor.close()
cnn.close

normaldistance = getwithoutridesharingdistance(coord_list)
ridesharedist = getwithridesharingdistance(carassign, carcount)
getsaveddistance(normaldistance, ridesharedist)
numpool = numpool + 1

starttime = Average.starttime
endtime = Average.endtime
numpool = 0
totaldistance = 0
tnormaldist = 0
tridesharedist = 0
wolength = 0
wlength = 0
programstarttime = datetime.datetime.now()

print("program start time:",programstarttime)
while endtime <= Average.untildatetime:
  print("\n",starttime,endtime)
  kmeanscluster(starttime,endtime)
  starttime = starttime + Average.windowsize
  endtime = endtime + Average.windowsize
  # print(starttime)
  # print(endtime)
programendtime = datetime.datetime.now()
print ("Total distance without ridesharing",tnormaldist,"km")
print ("Total distance with ridesharing",tridesharedist,"km")

print ("Average distance without ridesharing for eachpool",tnormaldist/numpool,"km")
print ("Average distance with ridesharing for each pool",tridesharedist/numpool,"km")

print ("\n Before RideSharing Total number of trips:",wolength/numpool)
print ("\n After RideSharing Total number of trips:",wlength/numpool)
print("\nNumber of pools:",numpool)

print("\nProgram End time",programendtime)
print ("Total time", programendtime - programstarttime)
print("Total distance saved(percentage):",round((totaldistance/numpool),2),"%")

