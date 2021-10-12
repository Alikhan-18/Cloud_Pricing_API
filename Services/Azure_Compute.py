import json
import cryptography
from cryptography.fernet import Fernet
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import row_number, lit
from pyspark.sql.functions import col, explode, lit
def Azure_Compute():
    key = b'dmnTQwD6a-YiyO8XgQTTnTQFH3xYSrFMTzpBrL2spIA='
    fernet = Fernet(key)
    enc_pwd = b'gAAAAABhVrNOKQEgiJyBIyL4QED2zocoz-NBhmVkrxkgtAvW9KrdLW708lLAOj_pLajlEBuBmtNLXwnNy5UGm5vGGtTnuHFdTQ=='
    contents = open("C:/Users/mkhan369/PycharmProjects/Spark_Examples/venv/Include/spark-warehouse/Sample/CONFIG_LOADER/config.txt").read()
    c = json.loads(contents)
    dec_pwd = fernet.decrypt(enc_pwd).decode()
    spark = SparkSession.builder.appName('Azure pricing').config("spark.jars","file:///C:/Users/mkhan369/Downloads/mysql-connector-java-8.0.26/mysql-connector-java-8.0.26.jar").getOrCreate()
    w = Window().orderBy(lit('A'))
    df = spark.read.json(c["AZ_loc"], multiLine=True)
    ddf = df.withColumn("new", F.arrays_zip("Meters", "OfferTerms")).withColumn("new", F.explode("new")).withColumn('created_on', F.lit("2021-09-02 05:15:59.533794")).\
    withColumn('Updated_On', F.current_timestamp()).withColumn('Created_by', F.lit("Team_Avengersr-ms")).withColumn("Updated_by", F.lit("Team_Avengers@gmail.com")).\
    withColumn("tco_pricing_azure_storage_id", row_number().over(w)).select(F.col("tco_pricing_azure_storage_id"), F.col("new.Meters.MeterCategory"), F.col("new.Meters.EffectiveDate"),
    F.col("new.Meters.MeterId"), F.col("new.Meters.MeterName"),F.col("new.Meters.MeterRates.0").alias("price_per_unit"),
    F.col("new.Meters.MeterRegion"), F.col("new.Meters.MeterStatus"), F.col("new.Meters.MeterSubCategory"), F.col("new.Meters.Unit"), F.col("created_on"), F.col("created_by"),
    F.col("Updated_on"), F.col("Updated_by"))
    dfinal=ddf
    dfinal.select("*").where(dfinal.MeterCategory=="Virtual Machines")
    df2 = dfinal.withColumn('yearly_ondemand_price', 24 * 365 * F.col('price_per_unit'))
    df3 = df2.withColumn('one_year_reservation', (1 / 1.666667) * F.col('yearly_ondemand_price'))
    df4 = df3.withColumn('three_year_reservation', (1 / 1.5) * F.col('one_year_reservation'))
    DF = df4
    DF.repartition(1).write.csv(c["outputAz_Com"],header=True)
    DF.write.mode("overwrite").format("jdbc").option("url", "jdbc:mysql://localhost:3306/pricing_api?useSSL=false").option( "driver", "com.mysql.jdbc.Driver").\
    option("dbtable", c["mysql"]["tableAZ_C"]).option("user", c["mysql"]["rootAZ_C"]).option("password",dec_pwd).save()
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!successfully stored in mysql!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
#
# if __name__ =="__main__":
#    Azure_Compute()