from pyspark.sql.functions import lit, max, expr, stddev, avg
import os
from pyspark.sql.types import StructType, StructField, StringType

complete_dataframe_schema = StructType([
    StructField('Serial_Number', StringType(), True),
    StructField('Family', StringType(), True),
    StructField('LUNG', StringType(), True),
    StructField('DIAM', StringType(), True),
    StructField('VibromPeak', StringType(), True),
    StructField('statorCelsius', StringType(), True),
    StructField('numTP', StringType(), True),
    StructField('idChiave', StringType(), True),
    StructField('statoRot', StringType(), True),
    StructField('rpmSetPoint', StringType(), True),
    StructField('bearingCelsius', StringType(), True),
    StructField('UTE', StringType(), True),
    StructField('spindleRpm', StringType(), True),
    StructField('CurrentPeak', StringType(), True),
    StructField('@timestamp', StringType(), True),
    StructField('ist_acc', StringType(), True),
])

family_max_dataframe_schema = StructType([
    StructField('Family', StringType(), True),
    StructField('VibromPeak_max', StringType(), True),
])

complete_dataframe = spark.createDataFrame([], complete_dataframe_schema)
family_max_dataframe = spark.createDataFrame([], family_max_dataframe_schema)

def dataset_index(directory):
    for index in range(len(directory)):
        if directory[index] == "Dataset":
            index_of_element = index
            return index_of_element


csv_extention = ".csv"
directory =  os.listdir()
print("------------------------------------------------------------------------")
print("\n")

for filename in os.scandir(directory[dataset_index(directory)]):
    folder_object = filename.path
    folder_object_name = folder_object.split("/")
    for filename in os.scandir(folder_object):
        subdirectory = filename.path
        subdirectory_name = subdirectory.split("/")
        serial_number_extracted = str(subdirectory_name[2])
        family_name_extracted = str(subdirectory_name[1])
        for filename in os.scandir(subdirectory):
            if filename.is_file():
                file_object = filename.path
                file_object_name = file_object.split("/")
                if csv_extention in file_object:
                    PATH = file_object
                    if(complete_dataframe.count() == 0):
                        complete_dataframe = spark.read.option("header",True).csv(PATH) \
                            .withColumn("Serial_Number", lit(serial_number_extracted)) \
                            .withColumn("Family", lit(family_name_extracted[:-7])) \
                            .select("Serial_Number", "Family", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", 
                                    "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", "@timestamp", 
                                    "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")
                    else:
                        complete_dataframe = complete_dataframe.union(spark.read.option("header",True).csv(PATH) \
                            .withColumn("Serial_Number", lit(serial_number_extracted)) \
                            .withColumn("Family", lit(family_name_extracted[:-7])) \
                            .select("Serial_Number", "Family", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", 
                                    "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", "@timestamp", 
                                    "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave"))

#Filtraggio delle righe null di VibromPeak e numTP

complete_dataframe = complete_dataframe.filter(complete_dataframe.VibromPeak.isNotNull())
complete_dataframe = complete_dataframe.filter(complete_dataframe.numTP.isNotNull())

#Cast della colonna VibromPeak da string a double in complete_dataframe

complete_dataframe = complete_dataframe.withColumn("VibromPeak", complete_dataframe["VibromPeak"].cast("double"))

#Cast della colonna VibromPeak da string a double in family_max_dataframe

family_max_dataframe = family_max_dataframe.withColumn("VibromPeak_max", family_max_dataframe["VibromPeak_max"].cast("double"))

#Calcolo del massimo di VibromPeak per tutte le famiglie di macchinari

print("Calcolo del massimo di VibromPeak per tutte le famiglie di macchinari")

family_max_dataframe = complete_dataframe.groupBy("Family").agg(max('VibromPeak').alias("VibromPeak_max")).show()

#Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni Serial_Number e numTP

print("Calcolo del massimo, mediana, devizione standard e media di VibromPeak per ogni Serial_Number e numTP di ogni macchinario")

family_sn_numTP_max_dataframe = complete_dataframe.groupBy("Family", "Serial_Number", "numTP") \
            .agg(max("VibromPeak"), stddev('VibromPeak'), avg("VibromPeak"), expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak median")) \
            .withColumnRenamed("max(VibromPeak)","VibromPeak max") \
            .withColumnRenamed("stddev_samp(VibromPeak)","VibromPeak stdDev") \
            .withColumnRenamed("avg(VibromPeak)","VibromPeak avg") \
            .orderBy("Family", "Serial_Number") \
            .show()

#Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni numTP

print("Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni numTP")

family_numTP_max_dataframe = complete_dataframe.groupBy("Family", "numTP") \
            .agg(max("VibromPeak"), stddev('VibromPeak'), avg("VibromPeak"), expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak median")) \
            .withColumnRenamed("max(VibromPeak)","VibromPeak max") \
            .withColumnRenamed("stddev_samp(VibromPeak)","VibromPeak stdDev") \
            .withColumnRenamed("avg(VibromPeak)","VibromPeak avg") \
            .orderBy("Family", "numTP") \
            .show()

#Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni Serial_Number

print("Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni Serial_Number")

family_sn_max_dataframe = complete_dataframe.groupBy("Family", "Serial_Number") \
            .agg(max("VibromPeak"), stddev('VibromPeak'), avg("VibromPeak"), expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak median"))\
            .withColumnRenamed("max(VibromPeak)","VibromPeak max") \
            .withColumnRenamed("stddev_samp(VibromPeak)","VibromPeak stdDev") \
            .orderBy("Family", "Serial_Number") \
            .show()
