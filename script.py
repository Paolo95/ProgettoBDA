from pyspark.sql.functions import lit, max, expr, stddev, avg, unix_timestamp
import os
import shutil
import time
import zipfile
from pyspark.sql.types import StructType, StructField, StringType

format = "yyyy-MM-dd'T'HH:mm:ss:SSSZ" 

start_time = time.time()

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

def folder_index(directory, name_selected_folder):
    for index in range(len(directory)):
        if directory[index] == str(name_selected_folder):
            index_of_element = index
            return index_of_element


directory =  os.listdir()

print("\n")

if "Output" in directory:
    print("Cartella di Output già presente, verrà eliminata")
    shutil.rmtree('Output')

print("------------------------------------------------------------------------")
print("\n")

for filename in os.scandir(directory[folder_index(directory, "Dataset")]):
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
                if file_object.endswith('.csv'):
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

family_max_dataframe = complete_dataframe.groupBy("Family").agg(max('VibromPeak').alias("VibromPeak_max"))

family_max_dataframe.show()

#Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni Serial_Number e numTP

print("Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni Serial_Number e numTP")

family_sn_numTP_dataframe = complete_dataframe.groupBy("Family", "Serial_Number", "numTP") \
            .agg(max("VibromPeak"), stddev('VibromPeak'), avg("VibromPeak"), expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak_median")) \
            .withColumnRenamed("max(VibromPeak)","VibromPeak_max") \
            .withColumnRenamed("stddev_samp(VibromPeak)","VibromPeak_stdDev") \
            .withColumnRenamed("avg(VibromPeak)","VibromPeak_avg") \
            .orderBy("Family", "Serial_Number")

family_sn_numTP_dataframe.show()

#Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni Family, UTE, Serial_Number e numTP

print("Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni Family, UTE, Serial_Number e numTP")

family_ute_sn_numTP_dataframe = complete_dataframe.groupBy("Family", "UTE", "Serial_Number", "numTP") \
            .agg(max("VibromPeak"), stddev('VibromPeak'), avg("VibromPeak"), expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak_median")) \
            .withColumnRenamed("max(VibromPeak)","VibromPeak_max") \
            .withColumnRenamed("stddev_samp(VibromPeak)","VibromPeak_stdDev") \
            .withColumnRenamed("avg(VibromPeak)","VibromPeak_avg") \
            .orderBy("Family", "Serial_Number")

family_ute_sn_numTP_dataframe.show()

#Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni numTP

print("Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni numTP")

family_numTP_dataframe = complete_dataframe.groupBy("Family", "numTP") \
            .agg(max("VibromPeak"), stddev('VibromPeak'), avg("VibromPeak"), expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak_median")) \
            .withColumnRenamed("max(VibromPeak)","VibromPeak_max") \
            .withColumnRenamed("stddev_samp(VibromPeak)","VibromPeak_stdDev") \
            .withColumnRenamed("avg(VibromPeak)","VibromPeak_avg") \
            .orderBy("Family", "numTP")

family_numTP_dataframe.show()

#Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni UTE e numTP

print("Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni UTE e numTP")

family_ute_numTP_dataframe = complete_dataframe.groupBy("Family", "UTE", "numTP") \
            .agg(max("VibromPeak"), stddev('VibromPeak'), avg("VibromPeak"), expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak_median")) \
            .withColumnRenamed("max(VibromPeak)","VibromPeak_max") \
            .withColumnRenamed("stddev_samp(VibromPeak)","VibromPeak_stdDev") \
            .withColumnRenamed("avg(VibromPeak)","VibromPeak_avg") \
            .orderBy("Family", "numTP")

family_ute_numTP_dataframe.show()

#Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni Serial_Number

print("Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni Serial_Number")

family_sn_dataframe = complete_dataframe.groupBy("Family", "Serial_Number") \
            .agg(max("VibromPeak"), stddev('VibromPeak'), avg("VibromPeak"), expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak_median"))\
            .withColumnRenamed("max(VibromPeak)","VibromPeak_max") \
            .withColumnRenamed("stddev_samp(VibromPeak)","VibromPeak_stdDev") \
            .withColumnRenamed("avg(VibromPeak)","VibromPeak_avg") \
            .orderBy("Family", "Serial_Number")

family_sn_dataframe.show()

#Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni UTE eSerial_Number

print("Calcolo del massimo, mediana, deviazione standard e media di VibromPeak per ogni UTE e Serial_Number")

family_ute_sn_dataframe = complete_dataframe.groupBy("Family", "UTE", "Serial_Number") \
            .agg(max("VibromPeak"), stddev('VibromPeak'), avg("VibromPeak"), expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak_median"))\
            .withColumnRenamed("max(VibromPeak)","VibromPeak_max") \
            .withColumnRenamed("stddev_samp(VibromPeak)","VibromPeak_stdDev") \
            .withColumnRenamed("avg(VibromPeak)","VibromPeak_avg") \
            .orderBy("Family", "Serial_Number")

family_ute_sn_dataframe.show()

#Cast della colonna @timestamp da string a timestamp

complete_dataframe = complete_dataframe.withColumn("@timestamp", unix_timestamp('@timestamp', format).cast('timestamp'))

#Esportazione dei CSV

complete_dataframe.coalesce(1) \
                    .write.option("header", "true") \
                    .csv("Output/Complete_dataset")
family_max_dataframe.coalesce(1) \
                    .write.option("header", "true") \
                    .csv("Output/Family_max")
family_sn_dataframe.coalesce(1) \
                    .write.option("header", "true") \
                    .csv("Output/Family_sn")
family_numTP_dataframe.coalesce(1) \
                    .write.option("header", "true") \
                    .csv("Output/Family_numTP")
family_sn_numTP_dataframe.coalesce(1) \
                    .write.option("header", "true") \
                    .csv("Output/Family_sn_numTP")
family_ute_numTP_dataframe.coalesce(1) \
                    .write.option("header", "true") \
                    .csv("Output/Family_ute_numTP")
family_ute_sn_dataframe.coalesce(1) \
                    .write.option("header", "true") \
                    .csv("Output/Family_ute_sn")
family_ute_sn_numTP_dataframe.coalesce(1) \
                    .write.option("header", "true") \
                    .csv("Output/Family_ute_sn_numTP")

#=========================================================
#Rinomino i file...


directory =  os.listdir()

for filename in os.scandir(directory[folder_index(directory, "Output")]):
    folder_object_name = folder_object.split("/")
    subdirectory = filename.path
    '''
    for filename in os.scandir(folder_object):
        subdirectory = filename.path
        subdirectory_name = subdirectory.split("/")
        serial_number_extracted = str(subdirectory_name[2])
        family_name_extracted = str(subdirectory_name[1])
        print(subdirectory_name)
    '''
    for filename in os.scandir(subdirectory):
        if filename.is_file():
            file_object = filename.path
            file_object_name = file_object.split("/")
            if file_object.endswith('.csv'):
                current_file = file_object
                current_directory = str(file_object_name[0]) + "/" + str(file_object_name[1]) + "/"
                renamed_file = str(file_object_name[1]) + str(".csv")
                new_file_object = current_directory  + renamed_file
                os.rename(current_file, new_file_object)
            else:
                os.remove(file_object)

def zip_dir(dirpath, zippath):
    fzip = zipfile.ZipFile(zippath, 'w', zipfile.ZIP_DEFLATED)
    basedir = os.path.dirname(dirpath) + '/' 
    for root, dirs, files in os.walk(dirpath):
        if os.path.basename(root)[0] == '.':
            continue #skip hidden directories        
        dirname = root.replace(basedir, '')
        for f in files:
            if f[-1] == '~' or (f[0] == '.' and f != '.htaccess'):
                #skip backup files and all hidden files except .htaccess
                continue
            fzip.write(root + '/' + f, dirname + '/' + f)
    fzip.close()

directory =  os.listdir()

if "Output.zip" in directory:
    print("Output.zip esiste, verrà eliminato")
    os.remove("Output.zip")
    output_filename = "Output"
    print("Compressione Output nel file Output.zip...")
    zip_dir("Output", "Output.zip")
    print("Output.zip creato.")
else:
    output_filename = "Output"
    print("Compressione Output nel file Output.zip...")
    zip_dir("Output", "Output.zip")
    print("Output.zip creato.")

def convert_time(sec):
    hour_str = ""
    minutes_str = ""
    secs_str = ""
    sec = sec % (24 * 3600)
    hour = sec // 3600
    sec %= 3600
    min = sec // 60
    sec %= 60
    if(hour == 0):
        hour_str = ""
    else:
        hour_str = str(int(hour)) + "h"
    if(min == 0):
        minutes_str = ""
    else:
        minutes_str = str(int(min)) + "m"
    secs_str = str(int(sec)) + "s"
    time_string = hour_str + minutes_str + secs_str
    return time_string

end_time = time.time()
time_passed = end_time - start_time
print("===========================================================")
print("Processo completato in : " + str(convert_time(time_passed)))
