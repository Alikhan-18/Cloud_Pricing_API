import requests
import json
import cryptography
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from cryptography.fernet import Fernet
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,lit,col,explode
from pyspark.sql.types import StructField, StructType, StringType, MapType,IntegerType
def Amazon_Compute():
    key = b'dmnTQwD6a-YiyO8XgQTTnTQFH3xYSrFMTzpBrL2spIA='
    fernet = Fernet(key)
    enc_pwd = b'gAAAAABhVrNOKQEgiJyBIyL4QED2zocoz-NBhmVkrxkgtAvW9KrdLW708lLAOj_pLajlEBuBmtNLXwnNy5UGm5vGGtTnuHFdTQ=='
    dec_pwd = fernet.decrypt(enc_pwd).decode()
    contents = open("C:/Users/mkhan369/PycharmProjects/Spark_Examples/venv/Include/spark-warehouse/Sample/CONFIG_LOADER/config.txt").read()
    c = json.loads(contents)
    dict1 = []
    f = open("C:/Users/mkhan369/PycharmProjects/Spark_Examples/venv/Include/spark-warehouse/Sample/CONFIG_LOADER/AmazonEC2.json","r")
    x1 = json.load(f)
    dict1.append(x1)
    spark = SparkSession.builder.appName('Cloud_Pricing_API').config("spark.jars","file:///C:/Users/mkhan369/Downloads/mysql-connector-java-8.0.26/mysql-connector-java-8.0.26.jar").getOrCreate()
    m1 = spark.createDataFrame(data=dict1, schema=eval(c['schema3']))
    s1 = m1.select(F.explode("products"))
    s1.createOrReplaceTempView("test")
    products = spark.sql("""select key,value['sku'] as sku,value['productFamily'] as product_Family,value['attributes']['marketoption'] as termtype,value['attributes']['servicecode'] as servivecode,
    value['attributes']['location'] as location,value['attributes']['instanceType'] as instanceType,value['attributes']['tenancy'] as tenancy,value['attributes']['operatingSystem'] as operatingSystem from test""")
    df_term = m1.select(explode('terms.onDemand')).select(explode('value')).select('value.*', 'value.termAttributes.*', explode('value.priceDimensions').alias(
    'k', 'v')).select('sku', 'effectiveDate', 'LeaseContractLength', 'OfferingClass', 'PurchaseOption',col('v.description').alias('price_description'), col('v.unit').alias('unit'),
    explode('v.pricePerUnit').alias('Currency', 'pricePerUnit'))
    finaldf = df_term.withColumn('created_on', F.lit("2021-09-01 15:33:34.405099")).withColumn('Updated_On', F.current_timestamp()).withColumn(
    'Created_by', F.lit("Team_Avengersr")).withColumn("Updated_by", F.lit("Team_Avengers"))
    compute_pricing = finaldf.join(products, 'sku')
    compute_pricing = compute_pricing.withColumn("yearly_ondemanded_pricing", 365 * F.col('pricePerUnit'))
    w = Window().orderBy('sku')
    compute_pricing.createOrReplaceTempView("aws")
    cp = spark.sql("select * from aws where product_Family in('Compute Instance','Compute Instance(bare metal)') ")
    finalcp = cp.withColumn("pricing_aws_compute_id", row_number().over(w))
    finalcp.createOrReplaceTempView('data')
    spark.sql("select pricing_aws_compute_id,sku,termtype,price_description,effectiveDate,unit,pricePerUnit,Currency,LeaseContractLength,PurchaseOption,OfferingClass,location,"
    "instanceType,operatingSystem,yearly_ondemanded_pricing,tenancy,created_on,Created_by,Updated_On,Updated_by from data").show(truncate=0)
    finalcp.repartition(1).write.csv(c["outputCompute"],header=True)
    finalcp.write.mode("overwrite").format("jdbc").option("url", "jdbc:mysql://localhost:3306/pricing_api?useSSL=false").option("driver", "com.mysql.jdbc.Driver").\
        option("dbtable", c["mysql"]["tableAM_C"]).option("user",c["mysql"]["rootAM_C"]).option("password",dec_pwd).save()
    print("!!!!!!!!!!!!!!!!!!!!!!successfully stored in mysql!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# if __name__ =="__main__":
# Amazon_Compute()