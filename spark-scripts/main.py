import os
import pyspark
from pyspark.sql.functions import col


def main():

    sparkcontext = pyspark.SparkContext.getOrCreate(
        conf=(
            pyspark
            .SparkConf()
            .setAppName('retail_app')
            .setMaster('spark://dataeng-spark-master:7077')
            .set('spark.jars', '/spark-scripts/postgresql-42.6.0.jar')
        )
    )
    sparkcontext.setLogLevel('WARN')

    spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

    postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
    postgres_user = os.getenv('POSTGRES_USER')
    postgres_password = os.getenv('POSTGRES_PASSWORD')
    postgres_db = os.getenv('POSTGRES_DB')

    jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_db}'
    jdbc_properties = {
        'user': postgres_user,
        'password': postgres_password,
        'driver': 'org.postgresql.Driver',
        'stringtype': 'unspecified'
    }


    def extract(spark, jdbc_url, jdbc_properties, table_name):

        data_frame = spark.read.jdbc(
            jdbc_url,
            table_name,
            properties=jdbc_properties
        )
        
        return data_frame
    

    def transform(df):

        df = df.withColumn('total_sales', df.quantity*df.unitprice)
        df = df.groupBy('country') \
               .sum('total_sales') \
               .withColumnRenamed('sum(total_sales)', 'total_sales') \
               .orderBy(col('total_sales').desc())
        
        return df
    

    def load(data_frame, jdbc_url, jdbc_properties, table_name):

        data_frame.write.mode('overwrite').jdbc(
        url=jdbc_url,
        table=table_name,
        mode='overwrite',
        properties=jdbc_properties)


    df_retail = extract(spark, jdbc_url, jdbc_properties, 'public.retail')
    df_retail.show(8)

    df_retail = transform(df_retail)
    df_retail.show(7)
    
    load(df_retail, jdbc_url, jdbc_properties, 'country_sales')

    spark.stop()

if __name__ == '__main__':
    main()
