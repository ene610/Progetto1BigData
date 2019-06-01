from pyspark import SparkConf
from pyspark import SparkContext
import datetime
import re

def parseLineHS(line):
    fields = re.compile(",\"|\",|,(?!.*\",|[ ])|,(?=[A-z]+,)").split(line)
    return fields[0],(fields[3])

def parseLinesHSP(line):
    fields = line.split(",")
    data = fields[7].split("-")
    if(fields[0]!="ticker"):
        return ((fields[0] , data[0]),(
                int(fields[6]),float(fields[2]),float(fields[2]),
                fields[7],fields[7],float(fields[2]),1
                ))

def reducerHSP(lineA,lineB):

    sumVolume = lineA[0] + lineB[0]

    if (lineA[3] < lineB[3]):
        firstClose = lineA[1]
        firstData = lineA[3]
    else:
        firstClose = lineB[1]
        firstData = lineB[3]

    if (lineA[4] > lineB[4]):
        lastClose = lineA[2]
        lastData = lineA[4]
    else:
        lastClose = lineB[2]
        lastData = lineB[4]

    sumClose = lineA[5] + lineB[5]
    sumOcc = lineA[6] + lineB[6]

    return sumVolume,firstClose,lastClose,firstData,lastData,sumClose,sumOcc


def risistemaHSP(line):
    return line[0][0],(line[0][1],line[1][0],line[1][1],line[1][2],line[1][5],line[1][6])

def risistemaHSjoinHSP(line):
    return ((line[1][1],line[1][0][0]),(line[1][0][1],line[1][0][2],line[1][0][3],line[1][0][4],line[1][0][5]))

def reducerJoin(lineaA,lineaB):
    sumVolume = lineaA[0]+lineaB[0]
    sumFirstClose = lineaA[1]+lineaB[1]
    sumLastClose = lineaA[2]+lineaB[2]
    sumSumClose = lineaA[3]+lineaB[3]
    sumSumCount = lineaA[4]+lineaB[4]

    return (sumVolume,sumFirstClose,sumLastClose,sumSumClose,sumSumCount)

def calcoloAumentoPercentualeECHiusuraMedia(line):
    sumVolume = line[0]
    aumentoPerc = int(((line[2]-line[1])/line[1])*100)
    avgClose = line[3]/line[4]

    return sumVolume,aumentoPerc,avgClose


conf = SparkConf().setAppName("Job2")

inizio2019 = "2019-01-01"
fine2013 = "2003-12-31"
sc = SparkContext(conf = conf)

inizio  = datetime.datetime.now()
linesHS = sc.textFile("file:///home/ene/Scrivania/Dati-primo-progetto/HS1.csv")
fieldsHS = linesHS.map(parseLineHS).filter(lambda x : x!=None and x[1]!="N/A")

linesHSP = sc.textFile("file:///home/ene/Scrivania/Dati-primo-progetto/HSP0.csv")

stage1 = linesHSP.map(parseLinesHSP).filter(lambda x : x!= None and x[1][3]<inizio2019 and x[1][3]>fine2013).reduceByKey(reducerHSP)
stage2= stage1.map(risistemaHSP).join(fieldsHS)
stage3= stage2.map(risistemaHSjoinHSP).reduceByKey(reducerJoin).mapValues(calcoloAumentoPercentualeECHiusuraMedia)

results = stage3.collect()

print(datetime.datetime.now()-inizio)

for result in results:
    print(results)

