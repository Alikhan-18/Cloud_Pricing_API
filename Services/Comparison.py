import mysql.connector as msql
import pandas as pd
from pyspark.sql import SparkSession
from mysql.connector import Error
import json
def pricing_comparitor():
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
    connection = msql.connect(host="localhost", database="pricing_api", user="root",password="Haseeb@1998")
    conn=connection
    cursor=conn.cursor()
    query="SELECT MeterName,yearly_ondemand_price,instanceType,yearly_ondemanded_pricing from azurecompute INNER JOIN amazonec2 on azurecompute.tco_pricing_azure_storage_id = amazonec2.pricing_aws_compute_id"
    pdf=pd.read_sql(query,con=conn)
    df=spark.createDataFrame(pdf)
    df_infra=spark.read.csv("C:/Users/mkhan369/PycharmProjects/Spark_Examples/venv/Include/spark-warehouse/Sample/CONFIG_LOADER/Infra_Intake.csv",header=True)
    df_infra.select("SKUAzure","SKUAWS")
    final_azure=df.join(df_infra,df.MeterName==df_infra.SKUAzure)
    final_azure.select("SKUAzure","yearly_ondemand_price")
    final_aws=df.join(df_infra,df.instanceType ==df_infra.SKUAWS)
    from pyspark.sql.functions import col
    final_aws=final_aws.filter(col("instanceType") == 'a1.2xlarge')
    merged_df=final_azure.unionByName(final_aws,allowMissingColumns=True)
    merged_df=merged_df.filter(col("instanceType") == 'a1.2xlarge')
    final=merged_df.select("SKUAzure","yearly_ondemand_price","SKUAWS","yearly_ondemanded_pricing")
    final.show()
pricing_comparitor()
#AWS & Azure comparator