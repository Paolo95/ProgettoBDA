import os
from os import walk
from os.path import splitext, join
from pyspark.sql.functions import lit, max, expr

def object_analysis(PATH, counter, folder_object_string, folder_num_string):
    #PATH = 'Dataset/Rover_B_FT_report/1000041225/logbiesse1000041225.csv'

    df_object = spark.read.option("header",True).csv(PATH)
    df_object = df_object.withColumn("Serial_Number", lit(folder_num_string))
    df_object = df_object.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
                "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")
    if(counter > 0):
        df_rover_bft = df_object.union(df_object).show()

#-------------------------------------Analisi dati---------------------------------------------------------
csv_extention = ".csv"
directory =  os.listdir()
print("-------------------------------------")
print(directory)
print("grandezza directory " + str(len(directory)))
print(directory[4])
print("\n")

for filename in os.scandir(directory[4]):
    counter = 0
    #if filename.is_file():
    folder_object = filename.path
    #print(folder_object) 
    folder_object_name = folder_object.split("/")
    print("1) " + str(folder_object_name[1])) #name of report's object
    for filename in os.scandir(folder_object):
        subdirectory = filename.path
        subdirectory_name = subdirectory.split("/")
        print("2)          " + str(subdirectory_name[2]))
        for filename in os.scandir(subdirectory):
            if filename.is_file():
                file_object = filename.path
                file_object_name = file_object.split("/")
                if csv_extention in file_object:#file_object_name[3]:
                    #print(file_object) #file_object da passare alla funzione
                    print("3)                   " + str(file_object_name[3]) + "|counter: " + str(counter))
                    PATH = file_object
                    object_analysis(PATH, counter, str(folder_object_name[1]), str(subdirectory_name[2]))
                    counter = counter + 1
                    #print("counter: " + str(counter))

#df_object = 
#df_object_aux =





'''
print("----Analisi dati su Rover_BF_T----\n")

PATH = 'Dataset/Rover_B_FT_report/1000037637/logbiesse1000037637.csv'

df_1000037637 = spark.read.option("header",True).csv(PATH)
df_1000037637 = df_1000037637.withColumn("Serial_Number", lit("1000037637"))
df_1000037637 = df_1000037637.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

PATH = 'Dataset/Rover_B_FT_report/1000041225/logbiesse1000041225.csv'

df_1000041225 = spark.read.option("header",True).csv(PATH)
df_1000041225 = df_1000041225.withColumn("Serial_Number", lit("1000041225"))
df_1000041225 = df_1000041225.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_bft = df_1000037637.union(df_1000041225)

PATH = 'Dataset/Rover_B_FT_report/1000041459/logbiesse1000041459.csv'

df_1000041459 = spark.read.option("header",True).csv(PATH)
df_1000041459 = df_1000041459.withColumn("Serial_Number", lit("1000041459"))
df_1000041459 = df_1000041459.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_bft = df_rover_bft.union(df_1000041459)

PATH = 'Dataset/Rover_B_FT_report/1000041964/logbiesse1000041964.csv'

df_1000041964 = spark.read.option("header",True).csv(PATH)
df_1000041964 = df_1000041964.withColumn("Serial_Number", lit("1000041964"))
df_1000041964 = df_1000041964.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_bft = df_rover_bft.union(df_1000041964)

PATH = 'Dataset/Rover_B_FT_report/1000041967/logbiesse1000041967.csv'

df_1000041967 = spark.read.option("header",True).csv(PATH)
df_1000041967 = df_1000041967.withColumn("Serial_Number", lit("1000041967"))
df_1000041967 = df_1000041967.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_bft = df_rover_bft.union(df_1000041967)

PATH = 'Dataset/Rover_B_FT_report/1000042109/logbiesse1000042109.csv'

df_1000042109 = spark.read.option("header",True).csv(PATH)
df_1000042109 = df_1000042109.withColumn("Serial_Number", lit("1000042109"))
df_1000042109 = df_1000042109.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_bft = df_rover_bft.union(df_1000042109)

PATH = 'Dataset/Rover_B_FT_report/1000043951/logbiesse1000043951.csv'

df_1000043951 = spark.read.option("header",True).csv(PATH)
df_1000043951 = df_1000043951.withColumn("Serial_Number", lit("1000043951"))
df_1000043951 = df_1000043951.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_bft = df_rover_bft.union(df_1000043951)

PATH = 'Dataset/Rover_B_FT_report/1000045424/logbiesse1000045424.csv'

df_1000045424 = spark.read.option("header",True).csv(PATH)
df_1000045424 = df_1000045424.withColumn("Serial_Number", lit("1000045424"))
df_1000045424 = df_1000045424.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_bft = df_rover_bft.union(df_1000045424)

PATH = 'Dataset/Rover_B_FT_report/1000045962/logbiesse1000045962.csv'

df_1000045962 = spark.read.option("header",True).csv(PATH)
df_1000045962 = df_1000045962.withColumn("Serial_Number", lit("1000045962"))
df_1000045962 = df_1000045962.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_bft = df_rover_bft.union(df_1000045962)

PATH = 'Dataset/Rover_B_FT_report/1000046482/logbiesse1000046482.csv'

df_1000046482 = spark.read.option("header",True).csv(PATH)
df_1000046482 = df_1000046482.withColumn("Serial_Number", lit("1000046482"))
df_1000046482 = df_1000046482.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_bft = df_rover_bft.union(df_1000046482)

#Filtraggio delle righe null di VibromPeak e numTP

df_rover_bft = df_rover_bft.filter(df_rover_bft.VibromPeak.isNotNull())
df_rover_bft = df_rover_bft.filter(df_rover_bft.numTP.isNotNull())

#Cast della colonna VibromPEak da string a double

df_rover_bft = df_rover_bft.withColumn("VibromPeak", df_rover_bft["VibromPeak"].cast("double"))

#Calcolo del massimo di VibromPeak per tutti i Rover BF_T

max_rover_bft_vibromPeak = df_rover_bft.select(max("VibromPeak")).collect()[0][0]

#Calcolo del massimo di VibromPeak per ogni Serial_Number e numTP

df_rover_bft.groupBy("Serial_Number", "numTP") \
            .max("VibromPeak") \
            .withColumnRenamed("max(VibromPeak)","VibromPeak max") \
            .show()

#Calcolo del massimo di VibromPeak per ogni numTP

df_rover_bft.groupBy("numTP") \
            .max("VibromPeak") \
            .withColumnRenamed("max(VibromPeak)","VibromPeak max") \
            .show()

#Calcolo del massimo di VibromPeak per ogni Serial_Number

df_rover_bft.groupBy("Serial_Number") \
            .max("VibromPeak") \
            .withColumnRenamed("max(VibromPeak)","VibromPeak max") \
            .show()

#Calcolo della mediana per ogni serial number

df_rover_bft.groupBy("Serial_Number") \
            .agg(expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak median")) \
            .show()

#Calcolo della mediana per ogni numTP

df_rover_bft.groupBy("numTP") \
            .agg(expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak median")) \
            .show()

#Calcolo della deviazione standard per ogni numTP

df_rover_bft.groupBy("numTP") \
            .agg({'VibromPeak': 'stddev'}) \
            .withColumnRenamed("stddev(VibromPeak)","VibromPeak stdDev") \
            .show()

#Calcolo della deviazione standard per ogni serial number

df_rover_bft.groupBy("Serial_Number") \
            .agg({'VibromPeak': 'stddev'}) \
            .withColumnRenamed("stddev(VibromPeak)","VibromPeak stdDev") \
            .show()

#Calcolo della deviazione standard per ogni Serial number e numTP

df_rover_bft.groupBy("Serial_Number", "numTP") \
            .agg({'VibromPeak': 'stddev'}) \
            .withColumnRenamed("stddev(VibromPeak)","VibromPeak stdDev") \
            .show()

#Calcolo della mediana per ogni Serial Number e numTP

df_rover_bft.groupBy("Serial_Number", "numTP") \
            .agg(expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak median")) \
            .show()

print("----Fine analisi dati Rover_BF_T----\n")

del df_1000037637
del df_1000041225
del df_1000041459
del df_1000041964
del df_1000041967
del df_1000042109
del df_1000043951
del df_1000045424
del df_1000045962
del df_1000046482
del df_rover_bft

#-------------------------------------Fine analisi Rover_BF_T-----------------------------------------------------------













#-------------------------------------Analisi dati su Rover_Edge---------------------------------------------------------

print("\n")
print("----Analisi dati su Rover_Edge----\n")

PATH = 'Dataset/Rover_Edge_report/1000036664/logbiesse1000036664.csv'

df_1000036664 = spark.read.option("header",True).csv(PATH)
df_1000036664 = df_1000036664.withColumn("Serial_Number", lit("1000036664"))
df_1000036664 = df_1000036664.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

PATH = 'Dataset/Rover_Edge_report/1000040598/logbiesse1000040598.csv'

df_1000040598 = spark.read.option("header",True).csv(PATH)
df_1000040598 = df_1000040598.withColumn("Serial_Number", lit("1000040598"))
df_1000040598 = df_1000040598.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_edge = df_1000036664.union(df_1000040598)

PATH = 'Dataset/Rover_Edge_report/1000041415/logbiesse1000041415.csv'

df_1000041415 = spark.read.option("header",True).csv(PATH)
df_1000041415 = df_1000041415.withColumn("Serial_Number", lit("1000041415"))
df_1000041415 = df_1000041415.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_edge = df_rover_edge.union(df_1000041415)

PATH = 'Dataset/Rover_Edge_report/1000042184/logbiesse1000042184.csv'

df_1000042184 = spark.read.option("header",True).csv(PATH)
df_1000042184 = df_1000042184.withColumn("Serial_Number", lit("1000042184"))
df_1000042184 = df_1000042184.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_edge = df_rover_edge.union(df_1000042184)

PATH = 'Dataset/Rover_Edge_report/1000043379/logbiesse1000043379.csv'

df_1000043379 = spark.read.option("header",True).csv(PATH)
df_1000043379 = df_1000043379.withColumn("Serial_Number", lit("1000043379"))
df_1000043379 = df_1000043379.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_edge = df_rover_edge.union(df_1000043379)

PATH = 'Dataset/Rover_Edge_report/1000044966/logbiesse1000044966.csv'

df_1000044966 = spark.read.option("header",True).csv(PATH)
df_1000044966 = df_1000044966.withColumn("Serial_Number", lit("1000044966"))
df_1000044966 = df_1000044966.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_edge = df_rover_edge.union(df_1000044966)

PATH = 'Dataset/Rover_Edge_report/1000045965/logbiesse1000045965.csv'

df_1000045965 = spark.read.option("header",True).csv(PATH)
df_1000045965 = df_1000045965.withColumn("Serial_Number", lit("1000045965"))
df_1000045965 = df_1000045965.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_edge = df_rover_edge.union(df_1000045965)

PATH = 'Dataset/Rover_Edge_report/1000046506/logbiesse1000046506.csv'

df_1000046506 = spark.read.option("header",True).csv(PATH)
df_1000046506 = df_1000046506.withColumn("Serial_Number", lit("1000046506"))
df_1000046506 = df_1000046506.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_edge = df_rover_edge.union(df_1000046506)

PATH = 'Dataset/Rover_Edge_report/1000047674/logbiesse1000047674.csv'

df_1000047674 = spark.read.option("header",True).csv(PATH)
df_1000047674 = df_1000047674.withColumn("Serial_Number", lit("1000047674"))
df_1000047674 = df_1000047674.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_edge = df_rover_edge.union(df_1000047674)

PATH = 'Dataset/Rover_Edge_report/1000049364/logbiesse1000049364.csv'

df_1000049364 = spark.read.option("header",True).csv(PATH)
df_1000049364 = df_1000049364.withColumn("Serial_Number", lit("1000049364"))
df_1000049364 = df_1000049364.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_edge = df_rover_edge.union(df_1000049364)

#Filtraggio delle righe null di VibromPeak e numTP

df_rover_edge = df_rover_edge.filter(df_rover_edge.VibromPeak.isNotNull())
df_rover_edge = df_rover_edge.filter(df_rover_edge.numTP.isNotNull())

#Cast della colonna VibromPEak da string a double

df_rover_edge = df_rover_edge.withColumn("VibromPeak", df_rover_edge["VibromPeak"].cast("double"))

#Calcolo del massimo di VibromPeak per tutti i Rover Edge

max_rover_edge_vibromPeak = df_rover_edge.select(max("VibromPeak")).collect()[0][0]

#Calcolo del massimo di VibromPeak per ogni Serial_Number e numTP

df_rover_edge.groupBy("Serial_Number", "numTP") \
            .max("VibromPeak") \
            .withColumnRenamed("max(VibromPeak)","VibromPeak max") \
            .show()

#Calcolo del massimo di VibromPeak per ogni numTP

df_rover_edge.groupBy("numTP") \
            .max("VibromPeak") \
            .withColumnRenamed("max(VibromPeak)","VibromPeak max") \
            .show()

#Calcolo del massimo di VibromPeak per ogni Serial_Number

df_rover_edge.groupBy("Serial_Number") \
            .max("VibromPeak") \
            .withColumnRenamed("max(VibromPeak)","VibromPeak max") \
            .show()

#Calcolo della mediana per ogni numTP

df_rover_edge.groupBy("numTP") \
            .agg(expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak median")) \
            .show()

#Calcolo della deviazione standard per ogni numTP

df_rover_edge.groupBy("numTP") \
            .agg({'VibromPeak': 'stddev'}) \
            .withColumnRenamed("stddev(VibromPeak)","VibromPeak stdDev") \
            .show()

#Calcolo della mediana per ogni serial number

df_rover_edge.groupBy("Serial_Number") \
            .agg(expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak median")) \
            .show()

#Calcolo della deviazione standard per ogni serial number

df_rover_edge.groupBy("Serial_Number") \
            .agg({'VibromPeak': 'stddev'}) \
            .withColumnRenamed("stddev(VibromPeak)","VibromPeak stdDev") \
            .show()

#Calcolo della deviazione standard per ogni Serial number e numTP

df_rover_edge.groupBy("Serial_Number", "numTP") \
            .agg({'VibromPeak': 'stddev'}) \
            .withColumnRenamed("stddev(VibromPeak)","VibromPeak stdDev") \
            .show()

#Calcolo della mediana per ogni Serial Number e numTP

df_rover_edge.groupBy("Serial_Number", "numTP") \
            .agg(expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak median")) \
            .show()

print("----Fine analisi dati Rover_Edge----\n")

del df_1000036664
del df_1000040598
del df_1000041415
del df_1000042184
del df_1000043379
del df_1000044966
del df_1000045965
del df_1000046506
del df_1000047674
del df_1000049364
del df_rover_edge

#-------------------------------------Fine analisi Rover_Edge-----------------------------------------------------------

















#-------------------------------------Analisi dati su Rover_Plast---------------------------------------------------------

print("\n")
print("----Analisi dati su Rover_Plast----\n")

PATH = 'Dataset/Rover_Plast_report/1000040499/logbiesse1000040499.csv'

df_1000040499 = spark.read.option("header",True).csv(PATH)
df_1000040499 = df_1000040499.withColumn("Serial_Number", lit("1000040499"))
df_1000040499 = df_1000040499.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

PATH = 'Dataset/Rover_Plast_report/1000040499/logbiesse1000040499.csv'

df_1000041221 = spark.read.option("header",True).csv(PATH)
df_1000041221 = df_1000041221.withColumn("Serial_Number", lit("1000041221"))
df_1000041221 = df_1000041221.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_plast = df_1000040499.union(df_1000041221)

PATH = 'Dataset/Rover_Plast_report/1000041541/logbiesse1000041541.csv'

df_1000041541 = spark.read.option("header",True).csv(PATH)
df_1000041541 = df_1000041541.withColumn("Serial_Number", lit("1000041541"))
df_1000041541 = df_1000041541.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_plast = df_rover_plast.union(df_1000041541)

PATH = 'Dataset/Rover_Plast_report/1000041551/logbiesse1000041551.csv'

df_1000041551 = spark.read.option("header",True).csv(PATH)
df_1000041551 = df_1000041551.withColumn("Serial_Number", lit("1000041551"))
df_1000041551 = df_1000041551.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_plast = df_rover_plast.union(df_1000041551)

PATH = 'Dataset/Rover_Plast_report/1000041934/logbiesse1000041934.csv'

df_1000041934 = spark.read.option("header",True).csv(PATH)
df_1000041934 = df_1000041934.withColumn("Serial_Number", lit("1000041934"))
df_1000041934 = df_1000041934.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_plast = df_rover_plast.union(df_1000041934)

PATH = 'Dataset/Rover_Plast_report/1000042898/logbiesse1000042898.csv'

df_1000042898 = spark.read.option("header",True).csv(PATH)
df_1000042898 = df_1000042898.withColumn("Serial_Number", lit("1000042898"))
df_1000042898 = df_1000042898.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_plast = df_rover_plast.union(df_1000042898)

PATH = 'Dataset/Rover_Plast_report/1000044949/logbiesse1000044949.csv'

df_1000044949 = spark.read.option("header",True).csv(PATH)
df_1000044949 = df_1000044949.withColumn("Serial_Number", lit("1000044949"))
df_1000044949 = df_1000044949.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_plast = df_rover_plast.union(df_1000044949)

PATH = 'Dataset/Rover_Plast_report/1000045111/logbiesse1000045111.csv'

df_1000045111 = spark.read.option("header",True).csv(PATH)
df_1000045111 = df_1000045111.withColumn("Serial_Number", lit("1000045111"))
df_1000045111 = df_1000045111.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_plast = df_rover_plast.union(df_1000045111)

PATH = 'Dataset/Rover_Plast_report/1000046652/logbiesse1000046652.csv'

df_1000046652 = spark.read.option("header",True).csv(PATH)
df_1000046652 = df_1000046652.withColumn("Serial_Number", lit("1000046652"))
df_1000046652 = df_1000046652.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_plast = df_rover_plast.union(df_1000046652)

PATH = 'Dataset/Rover_Plast_report/1000047557/logbiesse1000047557.csv'

df_1000047557 = spark.read.option("header",True).csv(PATH)
df_1000047557 = df_1000047557.withColumn("Serial_Number", lit("1000047557"))
df_1000047557 = df_1000047557.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_plast = df_rover_plast.union(df_1000047557)

#Filtraggio delle righe null di VibromPeak e numTP

df_rover_plast = df_rover_plast.filter(df_rover_plast.VibromPeak.isNotNull())
df_rover_plast = df_rover_plast.filter(df_rover_plast.numTP.isNotNull())

#Cast della colonna VibromPeak da string a double

df_rover_plast = df_rover_plast.withColumn("VibromPeak", df_rover_plast["VibromPeak"].cast("double"))

#Calcolo del massimo di VibromPeak per tutti i Rover Edge

max_rover_plast_vibromPeak = df_rover_plast.select(max("VibromPeak")).collect()[0][0]

#Calcolo del massimo di VibromPeak per ogni Serial_Number e numTP

df_rover_plast.groupBy("Serial_Number", "numTP") \
            .max("VibromPeak") \
            .withColumnRenamed("max(VibromPeak)","VibromPeak max") \
            .show()

#Calcolo del massimo di VibromPeak per ogni numTP

df_rover_plast.groupBy("numTP") \
            .max("VibromPeak") \
            .withColumnRenamed("max(VibromPeak)","VibromPeak max") \
            .show()

#Calcolo del massimo di VibromPeak per ogni Serial_Number

df_rover_plast.groupBy("Serial_Number") \
            .max("VibromPeak") \
            .withColumnRenamed("max(VibromPeak)","VibromPeak max") \
            .show()

#Calcolo della mediana per ogni numTP

df_rover_plast.groupBy("numTP") \
            .agg(expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak median")) \
            .show()

#Calcolo della deviazione standard per ogni numTP

df_rover_plast.groupBy("numTP") \
            .agg({'VibromPeak': 'stddev'}) \
            .withColumnRenamed("stddev(VibromPeak)","VibromPeak stdDev") \
            .show()

#Calcolo della mediana per ogni serial number

df_rover_plast.groupBy("Serial_Number") \
            .agg(expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak median")) \
            .show()

#Calcolo della deviazione standard per ogni Serial number e numTP

df_rover_plast.groupBy("Serial_Number", "numTP") \
            .agg({'VibromPeak': 'stddev'}) \
            .withColumnRenamed("stddev(VibromPeak)","VibromPeak stdDev") \
            .show()

#Calcolo della mediana per ogni Serial Number e numTP

df_rover_plast.groupBy("Serial_Number", "numTP") \
            .agg(expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak median")) \
            .show()

print("----Fine analisi dati Rover_Plast----\n")

del df_1000040499
del df_1000041221
del df_1000041541
del df_1000041551
del df_1000041934
del df_1000042898
del df_1000044949
del df_1000045111
del df_1000046652
del df_1000047557
del df_rover_plast

#-------------------------------------Fine analisi Rover_Plast-----------------------------------------------------------
'''