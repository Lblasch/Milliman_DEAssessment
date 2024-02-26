import argparse
from pyspark.sql import SparkSession
from stage_ICD import Stage_ICD
from stage_RX import Stage_RX
from final_ICD import Final_ICD
from final_RX import Final_RX

class Ingest:
    def __init__(self,
                 claims_source_path: str,
                 rx_source_path: str,
                 staging_folder: str
                 ) -> None:
        
        self.claims_source_path = claims_source_path
        self.rx_source_path = rx_source_path
        self.staging_folder = staging_folder
    
        self.spark = SparkSession.builder.getOrCreate()

    def run(self):
        # Stage Data
        stage_prob = Stage_ICD(self.claims_source_path, self.staging_folder).generate_staging_df()
        stage_prob.write.mode("overwrite").option("header","true").csv(staging_prob_folder)

        stage_rx = Stage_RX(self.rx_source_path, self.staging_folder).generate_staging_df()
        stage_rx.write.mode("overwrite").option("header","true").csv(staging_rx_folder)

        # FUTURE Update
        # ADD CCDA staging as a class as well

        # Merge to final folders
        final_icd = Final_ICD(self.staging_folder).create_final_df()
        final_icd.printSchema()
        # FUTURE UPDATE
        # Need to add step to merge data and not overwrite for incremental updates
        final_icd.coalesce(1).write.mode("overwrite").option("header","true").csv(args.final_output_folder + 'Problem.csv')


        final_rx = Final_RX(self.staging_folder).create_final_df()
        final_rx.printSchema()
        # FUTURE UPDATE
        # Need to add step to merge data and not overwrite for incremental updates
        final_rx.coalesce(1).write.mode("overwrite").option("header","true").csv(args.final_output_folder + 'Medication.csv')

if __name__ == '__main__':
    def argparser(argv=None):
        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument(
            "-cl", "--claims_source_file", help="Input Claims file path", type=str, required=True, default='./data/raw/data_engineer_exam_claims_final.csv' )
        parser.add_argument(
            "-r", "--rx_source_file", help="Input Medications Claims file", type=str, required=True, default ='./data/raw/data_engineer_exam_claims_final.csv' )
        parser.add_argument(
            "-s", "--staging_folder", help="Staging output folder", type=str, required=True, default='./data/staging/')
        parser.add_argument(
            "-f", "--final_output_folder", help="Final output folder", type=str, required=True, default='./data/final/')
        return parser.parse_args(argv)
    
    args = argparser()

    staging_prob_folder = args.staging_folder + 'staging_prob.csv'
    staging_rx_folder = args.staging_folder + 'staging_rx.csv'

    Ingest(claims_source_path = args.claims_source_file,
           rx_source_path = args.rx_source_file,
           staging_folder = args.staging_folder
           ).run()