import requests
import json
import cryptography
from cryptography.fernet import Fernet
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,lit,col,explode
from pyspark.sql.types import StructField, StructType, StringType, MapType,IntegerType
def Amazon_S3(service):
   key = b'dmnTQwD6a-YiyO8XgQTTnTQFH3xYSrFMTzpBrL2spIA=' # Encrypted Key
   fernet = Fernet(key)
   enc_pwd = b'gAAAAABhVrNOKQEgiJyBIyL4QED2zocoz-NBhmVkrxkgtAvW9KrdLW708lLAOj_pLajlEBuBmtNLXwnNy5UGm5vGGtTnuHFdTQ=='  #Encrypted password
   dec_pwd = fernet.decrypt(enc_pwd).decode()
   contents = open("C:/Users/mkhan369/PycharmProjects/Spark_Examples/venv/Include/spark-warehouse/Sample/CONFIG_LOADER/config.txt").read() #reading config
   dict1 = []
   c = json.loads(contents) #load config
   index_url = c["index_url"]
   response = requests.get(index_url).json()  #calling the api
   data = response['offers'] #searching for offercode
   service = data[service]
   current_url = service['currentVersionUrl']
   base_url = c["base_url"]
   service_url = base_url + current_url  #appending base & current url
   service_url = requests.get(service_url)  #calling the appended url
   x = json.loads(service_url.content.decode('utf8'))   #loading the url
   dict1.append(x)
   spark = SparkSession.builder.appName('Cloud_Pricing_API')\
   .config("spark.jars","file:///C:/Users/mkhan369/Downloads/mysql-connector-java-8.0.26/mysql-connector-java-8.0.26.jar").getOrCreate() #Spark Job
   m1 = spark.createDataFrame(data=dict1, schema=eval(c['S3schema'])) #Dataframe
   s1 = m1.select(F.explode("products")) #Explode function to parse
   s1.createOrReplaceTempView("test") #temporary table to run Queries
   products = spark.sql("""select value['sku'] as sku, value['productFamily'] as product_Family, value['attributes']['storageClass'] as storage_class, 
   value['attributes']['volumeType'] as volumeType, value['attributes']['location'] as location, value['attributes']['groupDescription'] as groupDescription, 
   value['attributes']['feeDescription'] as feeDescription from test""")
   df_term = m1.select(explode('terms.OnDemand')).select(explode('value')).select('value.*', explode('value.priceDimensions').alias('k', 'v')).select('sku',
   'effectiveDate',col('v.description').alias('price_description'),col('v.beginRange').alias('startingRange'),col('v.endRange').alias('EndRange'), col('v.unit').
   alias('unit'),explode('v.pricePerUnit').alias('Currency', 'pricePerUnit'))
   terms = df_term
   finaldf = terms.withColumn('created_on', F.lit("2021-09-02 05:15:59.533794")).withColumn('Updated_On',F.current_timestamp()).withColumn('Created_by',
   F.lit("Group_2@pwc.com")).withColumn("Updated_by", F.lit("Group_2@pwc.com")) #Extra Columns
   w = Window().orderBy(lit('A'))
   finaldf = finaldf.withColumn("blob_storage_pricing_id", row_number().over(w))
   pricing = finaldf.join(products, 'sku') #joining both using SKU
   pricing.createOrReplaceTempView("data")
   finalprice = spark.sql("select blob_storage_pricing_id ,product_Family,sku,price_description,groupDescription,feeDescription,effectiveDate,startingRange,EndRange,unit,"
   "pricePerUnit,Currency,location,storage_class,volumeType,created_on,Updated_On,Created_by,""Updated_by from data order by blob_storage_pricing_id ")

   finalprice.repartition(1).write.csv(c["outputS3"],header=True) #To Store in Local as CSV
   finalprice.write.mode("overwrite").format("jdbc").option("url","jdbc:mysql://localhost:3306/pricing_api?useSSL=false").option("driver", "com.mysql.jdbc.Driver")\
   .option("dbtable", c["mysql"]["tableAM_S3"]).option("user", c["mysql"]["rootAM_S3"]).option("password",dec_pwd).save() #JDBC for SQL Connection
   print("!!!!!!!!!!!!!!!!!!!!!!successfully stored in mysql!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
#
# if __name__ =="__main__":
#    Amazon_S3("AmazonS3")

