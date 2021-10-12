import requests
import json
import cryptography
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from cryptography.fernet import Fernet
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,lit,col,explode
from pyspark.sql.types import StructField, StructType, StringType, MapType,IntegerType
def Amazon_EBS():
    key = b'dmnTQwD6a-YiyO8XgQTTnTQFH3xYSrFMTzpBrL2spIA='
    fernet = Fernet(key)
    enc_pwd = b'gAAAAABhVrNOKQEgiJyBIyL4QED2zocoz-NBhmVkrxkgtAvW9KrdLW708lLAOj_pLajlEBuBmtNLXwnNy5UGm5vGGtTnuHFdTQ=='
    dec_pwd = fernet.decrypt(enc_pwd).decode()
    contents = open("C:/Users/mkhan369/PycharmProjects/Spark_Examples/venv/Include/spark-warehouse/Sample/CONFIG_LOADER/config.txt").read()
    c = json.loads(contents)
    dict1 = []
    f = open("C:/Users/mkhan369/PycharmProjects/Spark_Examples/venv/Include/spark-warehouse/Sample/CONFIG_LOADER/EBS.json","r")
    x1 = json.load(f)
    dict1.append(x1)
    spark = SparkSession.builder.appName('Clou_pricing_API').config("spark.jars", "file:///C:/Users/mkhan369/Downloads/mysql-connector-java-8.0.26/mysql-connector-java-8.0.26.jar").getOrCreate()
    m1 = spark.createDataFrame(data=dict1, schema=eval(c['EBSschema']))
    s1 = m1.select(F.explode("products"))
    s1.createOrReplaceTempView("test")
    products = spark.sql(
    """select key, value['sku'] as sku, value['productFamily'] as product_Family, value['attributes']['location'] as location, value['attributes']['storageMedia'] as
    storageMedia, value['attributes']['volumeType'] as volumeType, value['attributes']['maxVolumeSize'] as maxVolumeSize, value['attributes']['marketoption'] as termtype, value['attributes']['maxIopsvolume'] as maxIopsvolume,
    value['attributes']['maxThroughputvolume'] as maxThroughputvolume, value['attributes']['maxIopsBurstPerformance'] as IopsBurstPerformance, value['attributes']['volumeApiName'] as volumeApiName from test """)
    df = m1.select(explode('terms.OnDemand')).select(explode('value')).select('value.*',explode('value.priceDimensions').alias('x', 'y')).select('sku','effectiveDate',
    col('y.unit').alias('unit'), col('y.description').alias('Price_description'),explode('y.pricePerUnit').alias( 'Currency','pricePerUnit'))
    finaldf = df.withColumn('created_on', F.lit("2021-09-01 15:33:34.405099")).withColumn('Updated_On',F.current_timestamp()).withColumn('Created_by', F.lit("Group_2@gmail.com")).withColumn("Updated_by", F.lit("Group_2"))
    EBSpricing = finaldf.join(products, 'sku')
    EBSpricing = EBSpricing.select('*').where(EBSpricing.product_Family == 'Storage')
    w = Window().orderBy('sku')
    EBSpricing = EBSpricing.withColumn("pricing_aws_ebs_id", row_number().over(w))
    EBSpricing.createOrReplaceTempView("data")
    final=spark.sql("select pricing_aws_ebs_id,maxVolumeSize,sku as key,Price_description,IopsBurstPerformance,unit,Currency,pricePerUnit,location,\
    storageMedia,volumeType,termtype,maxThroughputvolume,volumeApiName,effectiveDate,created_on,Created_by,Updated_On,Updated_by from data")
    final.show(truncate=0)
    final.repartition(1).write.csv(c["outputEBS"],header=True)
    final.write.mode("overwrite").format("jdbc").option("url", "jdbc:mysql://localhost:3306/pricing_api?useSSL=false").option(
    "driver", "com.mysql.jdbc.Driver").option("dbtable", c["mysql"]["tableAM_EBS"]).option("user",c["mysql"]["rootAM_EBS"]).option("password",dec_pwd).save()
    print("!!!!!!!!!!!!!!!!!!!!!!successfully stored in mysql!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# if __name__ =="__main__":
#     Amazon_EBS()