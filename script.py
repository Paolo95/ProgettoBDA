from pyspark.sql.functions import lit, max, expr

PATH = '/home/andrea/Desktop/Progetto_BDA/Dataset/Rover_B_FT_report/1000037637/logbiesse1000037637.csv'

df_1000037637 = spark.read.option("header",True).csv(PATH)
df_1000037637 = df_1000037637.withColumn("Serial_Number", lit("1000037637"))
df_1000037637 = df_1000037637.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

PATH = '/home/andrea/Desktop/Progetto_BDA/Dataset/Rover_B_FT_report/1000041225/logbiesse1000041225.csv'

df_1000041225 = spark.read.option("header",True).csv(PATH)
df_1000041225 = df_1000041225.withColumn("Serial_Number", lit("1000041225"))
df_1000041225 = df_1000041225.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_bft = df_1000037637.union(df_1000041225)

PATH = '/home/andrea/Desktop/Progetto_BDA/Dataset/Rover_B_FT_report/1000041459/logbiesse1000041459.csv'

df_1000041459 = spark.read.option("header",True).csv(PATH)
df_1000041459 = df_1000041459.withColumn("Serial_Number", lit("1000041459"))
df_1000041459 = df_1000041459.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_bft = df_rover_bft.union(df_1000041459)

PATH = '/home/andrea/Desktop/Progetto_BDA/Dataset/Rover_B_FT_report/1000041964/logbiesse1000041964.csv'

df_1000041964 = spark.read.option("header",True).csv(PATH)
df_1000041964 = df_1000041964.withColumn("Serial_Number", lit("1000041964"))
df_1000041964 = df_1000041964.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_bft = df_rover_bft.union(df_1000041964)

PATH = '/home/andrea/Desktop/Progetto_BDA/Dataset/Rover_B_FT_report/1000041967/logbiesse1000041967.csv'

df_1000041967 = spark.read.option("header",True).csv(PATH)
df_1000041967 = df_1000041967.withColumn("Serial_Number", lit("1000041967"))
df_1000041967 = df_1000041967.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_bft = df_rover_bft.union(df_1000041967)

PATH = '/home/andrea/Desktop/Progetto_BDA/Dataset/Rover_B_FT_report/1000042109/logbiesse1000042109.csv'

df_1000042109 = spark.read.option("header",True).csv(PATH)
df_1000042109 = df_1000042109.withColumn("Serial_Number", lit("1000042109"))
df_1000042109 = df_1000042109.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_bft = df_rover_bft.union(df_1000042109)

PATH = '/home/andrea/Desktop/Progetto_BDA/Dataset/Rover_B_FT_report/1000043951/logbiesse1000043951.csv'

df_1000043951 = spark.read.option("header",True).csv(PATH)
df_1000043951 = df_1000043951.withColumn("Serial_Number", lit("1000043951"))
df_1000043951 = df_1000043951.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_bft = df_rover_bft.union(df_1000043951)

PATH = '/home/andrea/Desktop/Progetto_BDA/Dataset/Rover_B_FT_report/1000045424/logbiesse1000045424.csv'

df_1000045424 = spark.read.option("header",True).csv(PATH)
df_1000045424 = df_1000045424.withColumn("Serial_Number", lit("1000045424"))
df_1000045424 = df_1000045424.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

PATH = '/home/andrea/Desktop/Progetto_BDA/Dataset/Rover_B_FT_report/1000045962/logbiesse1000045962.csv'

df_1000045962 = spark.read.option("header",True).csv(PATH)
df_1000045962 = df_1000045962.withColumn("Serial_Number", lit("1000045962"))
df_1000045962 = df_1000045962.select("Serial_Number", "VibromPeak", "UTE", "ist_acc", "bearingsCelsius", "rpmSetPoint", "statoRot", "statorCelsius", "spindleRpm", 
              "@timestamp", "numTP", "CurrentPeak", "DIAM", "LUNG", "idChiave")

df_rover_bft = df_rover_bft.union(df_1000045962)

PATH = '/home/andrea/Desktop/Progetto_BDA/Dataset/Rover_B_FT_report/1000046482/logbiesse1000046482.csv'

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

#Calcolo del massimo di VibromPeak per ogni Serial_Number

df_rover_bft.groupBy("Serial_Number") \
            .max("VibromPeak") \
            .withColumnRenamed("max(VibromPeak)","VibromPeak max") \
            .show()

#Calcolo della mediana per ogni serial number

df_rover_bft.groupBy("Serial_Number") \
            .agg(expr('percentile(VibromPeak, array(0.5))')[0].alias("VibromPeak median")) \
            .show()

#Calcolo della deviazione standard per ogni serial number

df_rover_bft.groupBy("Serial_Number") \
            .agg({'VibromPeak': 'stddev'}) \
            .withColumnRenamed("stddev(VibromPeak)","VibromPeak stdDev") \
            .show()

