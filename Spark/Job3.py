from pyspark import SparkConf
from pyspark import SparkContext
import datetime
import re

def parseLineHS(line):
    fields = re.compile(",(?!.*\")|\",|,\"|,(?=.*,\")").split(line)
    if(fields[0]!="ticker"):
        return fields[0],(fields[3],fields[2])

def parseLinesHSP(line):
    fields = line.split(",")
    data = fields[7].split("-")

    if(fields[0]=="ticker"):
        return None
    return ((fields[0] , data[0]),(
            float(fields[2]),float(fields[2]),
            fields[7],fields[7]
            ))

def reduceHSP(lineA,lineB):
    if (lineA[2] < lineB[2]):
        firstClose = lineA[0]
        firstData = lineA[2]
    else:
        firstClose = lineB[0]
        firstData = lineB[2]

    if (lineA[3] > lineB[3]):
        lastClose = lineA[1]
        lastData = lineA[3]
    else:
        lastClose = lineB[1]
        lastData = lineB[3]

    return firstClose, lastClose, firstData, lastData

def riordinaHSP(line):
    firstClose2016,firstClose2017,firstClose2018 = 0,0,0
    lastClose2016,lastClose2017,lastClose2018 = 0,0,0


    if(line[0][1] == '2016'):
        firstClose2016 = line[1][0]
        lastClose2016 = line[1][1]

    if (line[0][1] == '2017'):
        firstClose2017 = line[1][0]
        lastClose2017 = line[1][1]

    if (line[0][1] == '2018'):
        firstClose2018 = line[1][0]
        lastClose2018 = line[1][1]

    return line[0][0],(firstClose2016,lastClose2016,firstClose2017,lastClose2017,firstClose2018,lastClose2018)

def lastOnHSP(lineA, lineB):
    return lineA[0] + lineB[0], \
           lineA[1] + lineB[1], \
           lineA[2] + lineB[2], \
           lineA[3] + lineB[3], \
           lineA[4] + lineB[4], \
           lineA[5] + lineB[5]

def riordinaHSjoinHSP(line):
    return line[1][1][1],(line[1][1][0],line[1][0][0],line[1][0][1],line[1][0][2],line[1][0][3],line[1][0][4],line[1][0][5])

def reduceJoin(fieldsA,fieldsB):
    sumFirstClose2016 = fieldsA[1] + fieldsB[1]
    sumFirstClose2017 = fieldsA[3] + fieldsB[3]
    sumFirstClose2018 = fieldsA[5] + fieldsB[5]
    sumLastClose2016 = fieldsA[2] + fieldsB[2]
    sumLastClose2017 = fieldsA[4] + fieldsB[4]
    sumLastClose2018 = fieldsA[6] + fieldsB[6]

    return fieldsA[0],sumFirstClose2016,sumLastClose2016,sumFirstClose2017,sumLastClose2017,sumFirstClose2018,sumLastClose2018

def eliminaZeri(line):
    if(line[1][1] == 0 or line[1][3] == 0 or line[1][5] == 0 ):
        return False
    return True

def percentualiKey(line):
    aumentoPercentuale2016 = int(((line[1][2] - line[1][1]) / line[1][1]) * 100)
    aumentoPercentuale2017 = int(((line[1][4] - line[1][3]) / line[1][3]) * 100)
    aumentoPercentuale2018 = int(((line[1][6] - line[1][5]) / line[1][5]) * 100)

    return (aumentoPercentuale2016,aumentoPercentuale2017,aumentoPercentuale2018) ,(line[1][0],line[0])

inizio2019 = "2019-01-01"
fine2013 = "2015-12-31"

conf = SparkConf().setAppName("EsB")
sc = SparkContext(conf = conf)

inizio  = datetime.datetime.now()

linesHS = sc.textFile("file:///home/ene/Scrivania/Dati-primo-progetto/HS3.csv")
fieldsHS = linesHS.map(parseLineHS).filter(lambda x : x!=None and x[1][0]!="N/A")

linesHSP = sc.textFile("file:///home/ene/Scrivania/Dati-primo-progetto/HSP3.csv")

stage1 = linesHSP.map(parseLinesHSP).filter(lambda x : x != None and x[1][3]<inizio2019 and x[1][3]>fine2013).reduceByKey(reduceHSP)
stage2 = stage1.map(riordinaHSP).reduceByKey(lastOnHSP)
stage3 = stage2.join(fieldsHS).map(riordinaHSjoinHSP).reduceByKey(reduceJoin)
stage4 = stage3.filter(eliminaZeri).map(percentualiKey)
stage5 = stage4.join(stage4).filter(lambda x : x[1][0][0]!=x[1][1][0]).mapValues(lambda x: (x[0][1],x[1][1]))

output = stage5.collect()

print(datetime.datetime.now()-inizio)

for result in output:
    print(result)
