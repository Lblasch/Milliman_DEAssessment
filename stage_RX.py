from pyspark.sql import SparkSession
import pyspark.sql.functions as f



class Stage_RX:
    def __init__(self,
                 rx_source_path: str, 
                 staging_folder: str) -> None:

        self.rx_source_path = rx_source_path
        self.staging_folder = staging_folder
        self.spark = SparkSession.builder.getOrCreate()

    def  read_input(self):
        self.rx_raw_file = self.spark.read.option("inferSchema",True).csv(self.rx_source_path, header=True, sep=',')

    def stage_rx_data(self):
        df = self.rx_raw_file\
                   .withColumnRenamed('NDC', 'CODE')\
                   .withColumn('SUPPLEMENTAL_DATA', f.lit(False))\
                   .withColumn('CODE_SYSTEM', f.lit('NDC'))\
                   .select('MEMBERID',
                           'CLAIMID',
                           'DaysSupply',
                           'QuantityDispensed',
                           'SUPPLEMENTAL_DATA',
                           'CODE',
                           'CODE_SYSTEM')
        return df

    def generate_staging_df(self):
        self.read_input()
        final_df = self.stage_rx_data()
        return final_df






