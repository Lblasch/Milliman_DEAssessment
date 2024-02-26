from pyspark.sql import SparkSession
import pyspark.sql.functions as f

class Final_RX:
    def __init__(self,
                 staging_folder: str) -> None:

        self.staging_folder = staging_folder
        self.spark = SparkSession.builder.getOrCreate()

    def  read_ccda_staging(self):
        staging_ccda_prob = self.staging_folder + 'staging_ccda_med.csv'
        self.ccda_staging = self.spark.read.option("inferSchema",True).csv(staging_ccda_prob, header=True, sep=',')

    def read_claims_staging(self):
        staging_prob = self.staging_folder + 'staging_rx.csv'
        self.claims_staging = self.spark.read.option("inferSchema",True).csv(staging_prob, header=True, sep=',').drop('FROMDATE')

    def create_final_df(self):
        self.read_ccda_staging()
        self.read_claims_staging()
        final_rx_df = self.claims_staging.union(self.ccda_staging)
        return final_rx_df


