from pyspark import SparkConf
from pyspark import SparkContext
import datetime


def parseLines(line):
    fields = line.split(",")
    data = fields[7].split("-")
    if(fields[0]!= "ticker"):
        return (fields[0],(
            float(fields[4]),float(fields[5]),(int(fields[6]),1),fields[7],fields[7],
            float(fields[2]),float(fields[2])))

def reduceByTicker(tupla1, tupla2):

    if(tupla1[0]<tupla2[0]):
        lowThe = tupla1[0]
    else:
        lowThe = tupla2[0]

    if(tupla1[1]>tupla2[1]):
        highThe = tupla1[1]
    else:
        highThe = tupla2[1]

    volumeCount = (tupla1[2][0] + tupla2[2][0],tupla1[2][1] + tupla2[2][1])

    if(tupla1[3]<tupla2[3]):
        firstClose = tupla1[5]
        dataFirstClose = tupla1[3]
    else:
        firstClose = tupla2[5]
        dataFirstClose = tupla2[3]

    if (tupla1[4] > tupla2[4]):
        lastClose = tupla1[6]
        dataLastClose = tupla1[4]
    else:
        lastClose = tupla2[6]
        dataLastClose = tupla2[4]

    return (lowThe,highThe,volumeCount,dataFirstClose,dataLastClose,firstClose,lastClose)

def CalcoloAVGeAumentoPercentuale(tupla):
    aumentoPercentuale = int(((tupla[6]-tupla[5])/tupla[5]) * 100)
    avgVolumeMedio = (tupla[2][0]/tupla[2][1])
    return aumentoPercentuale,tupla[0],tupla[1],avgVolumeMedio

inizio2019 = "2019-01-01"
fine1997 = "1997-12-31"
conf = SparkConf().setAppName("Job1")
sc = SparkContext(conf = conf)
inizio  = datetime.datetime.now()
linesHSP = sc.textFile("file:///home/ene/Scrivania/Dati-primo-progetto/HSP0.csv")

stage1 = linesHSP.map(parseLines).filter(lambda x : x!=None and x[1][3]<inizio2019 and x[1][3]>fine1997).\
    reduceByKey(reduceByTicker)

top10 =stage1.mapValues(CalcoloAVGeAumentoPercentuale).sortBy(lambda a : a[1][0], False).take(10)

results = top10

print(datetime.datetime.now()-inizio)

for result in results:
    print(" Ticker: ",result[0],
          " Aumento Percentuale: ",result[1][0],
          " low: ",result[1][1],
          " high: ",result[1][2],
          "Volume medio: ",result[1][3])

