from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import *

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Abc").getOrCreate()

schemaairline = StructType([
        StructField("airline_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("alias1", StringType(), True),
        StructField("iata", StringType(), True),
        StructField("icao", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("country", StringType(), True),
        StructField("active", StringType(), True)
    ])

airline = spark.read.csv(r"gs://raw_from_orcles/airline.csv", schema=schemaairline)

airline.createOrReplaceTempView("airline")


airline.createOrReplaceTempView("airline")
airline1 = airline.withColumn('alias1',when(airline.alias1 == '\\N','Unknown').otherwise(airline.alias1))

airline1_1 = airline1.withColumn('iata',when(airline1.iata == '\\N','Unknown').otherwise(airline1.iata))

airline1_2 = airline1_1.withColumn('icao',when(airline1_1.icao == '\\N','Unknown').otherwise(airline1_1.icao))

airline1_3 = airline1_2.withColumn('callsign',when(airline1_2.callsign == '\\N','Unknown').otherwise(airline1_2.callsign))

airline1_4 = airline1_3.withColumn('country',when(airline1_3.country == '\\N','Unknown').otherwise(airline1_3.country))

airline1_5 = airline1_4.withColumn('active',when(airline1_4.active == '\\N','Unknown').otherwise(airline1_4.active))


airline3 = airline1_5.na.fill("Unknown",["name","iata","icao","callsign","country","active"])  

airline4 = airline3.na.fill(value = -1)


airline4.write.format("csv").option("path",f"gs://process_data1//Reprocess_airline1.csv").save(header = True)



