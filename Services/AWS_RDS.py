import requests
import json
import cryptography
from cryptography.fernet import Fernet
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,lit,col,explode
from pyspark.sql.types import StructField, StructType, StringType, MapType,IntegerType
def Amazon_RDS(service):
    key = b'dmnTQwD6a-YiyO8XgQTTnTQFH3xYSrFMTzpBrL2spIA='
    fernet = Fernet(key)
    enc_pwd = b'gAAAAABhVrNOKQEgiJyBIyL4QED2zocoz-NBhmVkrxkgtAvW9KrdLW708lLAOj_pLajlEBuBmtNLXwnNy5UGm5vGGtTnuHFdTQ=='
    dec_pwd = fernet.decrypt(enc_pwd).decode()
    contents = open("C:/Users/mkhan369/PycharmProjects/Spark_Examples/venv/Include/spark-warehouse/Sample/CONFIG_LOADER/config.txt").read()
    dict1 = []
    c = json.loads(contents)
    index_url = c["index_url"]
    response = requests.get(index_url).json()
    data = response['offers']
    service = data[service]
    current_url = service['currentVersionUrl']
    base_url=c["base_url"]
    service_url = base_url + current_url
    service_url = requests.get(service_url)
    x = json.loads(service_url.content.decode('utf8'))
    dict1.append(x)
    spark = SparkSession.builder.appName('SparkByExamples.com').config("spark.jars", "file:///C:/Users/mkhan369/Downloads/mysql-connector-java-8.0.26/mysql-connector-java-8.0.26.jar")\
    .getOrCreate()
    m1 = spark.createDataFrame(data=dict1, schema=eval(c["rds1"]))
    m2 = spark.createDataFrame(data=dict1, schema=eval(c["rds2"]))
    w = Window().orderBy(lit('A'))
    s1 = m1.select(F.explode("products"))
    s1.createOrReplaceTempView("test")
    products = spark.sql("""select value['sku'] as sku, value['productFamily'] as product_Family,value['attributes']['location'] as location, value['attributes']
    ['instanceFamily'] as instanceFamily, value['attributes']['currentGeneration'] as currentGeneration,value['attributes']['vcpu'] as vcpu,value['attributes']
    ['instanceType'] as instanceType, value['attributes']['databaseEngine'] as databaseEngine, value['attributes']['databaseEdition'] as databaseEdition,value['attributes']
    ['licenseModel'] as licenseModel, value['attributes']['deploymentOption'] as  deploymentOption, value['attributes']['memory'] as  memory from test""")
    df_term = m1.select(explode('terms.Reserved')).select(explode('value')).select('value.*', 'value.termAttributes.*', explode('value.priceDimensions').alias('k', 'v')).\
    select('sku','effectiveDate','LeaseContractLength','OfferingClass', 'PurchaseOption',col('v.description').alias('price_description'),col('v.unit').alias('unit'),
    explode('v.pricePerUnit').alias('Currency','pricePerUnit'))
    termsreserved = df_term.withColumn('term_type', F.lit('Reserved'))
    df_term = m2.select(explode('terms.OnDemand')).select(explode('value')).select('value.*', 'value.termAttributes.*',explode('value.priceDimensions').alias('k', 'v')).\
    select('sku', 'effectiveDate','LeaseContractLength','OfferingClass','PurchaseOption',col('v.description').alias('price_description'), col('v.unit').alias( 'unit'),
    explode('v.pricePerUnit').alias('Currency','pricePerUnit'))
    termsOn = df_term.withColumn('term_type', F.lit('OnDemand'))
    finalterms = termsreserved.union(termsOn)
    pricing = finalterms.join(products, 'sku')
    finalpricing = pricing.withColumn('created_on', F.lit("2021-09-02 05:15:59.533794")).withColumn('Updated_On',F.current_timestamp()).withColumn('Created_by',
    F.lit("Group_2")).withColumn("Updated_by", F.lit("Group_2@pwc.com"))
    finalpricing = finalpricing.withColumn("tco_pricing_aws_rds_id", row_number().over(w))
    finalpricing.createOrReplaceTempView('aws')
    final = spark.sql('select  tco_pricing_aws_rds_id,sku,term_type,price_description,effectiveDate,''unit,pricePerUnit,Currency,LeaseContractLength,PurchaseOption,'
    'OfferingClass,''location,instanceType,currentGeneration,instanceFamily,vcpu,memory,databaseEngine,databaseEdition,''licenseModel,deploymentOption ,created_on,'
    'Updated_On,Created_by,Updated_by from aws')
    #Final=final.show(truncate=0)
    final.repartition(1).write.csv(c["outputRDS"],header=True)
    final.write.mode("overwrite").format("jdbc").option("url","jdbc:mysql://localhost:3306/Pricing_api?allowPublicKeyRetrieval=true&useSSL=false&serverTimeZone=UTC").option("driver", "com.mysql.jdbc.Driver").\
    option("dbtable",c["mysql"]["tableAM_RDS"]).option("user",c["mysql"]["rootAM_RDS"]).option("password",dec_pwd).save()
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!successfully stored in mysql!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# if __name__ =="__main__":
#    Amazon_RDS("AmazonRDS")
