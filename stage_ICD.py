from pyspark.sql import SparkSession
import pyspark.sql.functions as f



class Stage_ICD:
    def __init__(self,
                 claims_source_path: str, 
                 staging_folder: str) -> None:

        self.claims_source_path = claims_source_path
        self.staging_folder = staging_folder
        self.spark = SparkSession.builder.getOrCreate()

    def  read_input(self):
        self.icd_raw_file = self.spark.read.option("inferSchema",True).csv(self.claims_source_path, header=True, sep=',')

    def stage_claim_problem_data(self):
        df = self.icd_raw_file.withColumn('ICDS', f.array(f.col('ICDDiag1'),
                                       f.col('ICDDiag2'),
                                       f.col('ICDDiag3'),
                                       f.col('ICDDiag4'),
                                       f.col('ICDDiag5'),
                                       f.col('ICDDiag6'),
                                       f.col('ICDDiag7'),
                                       f.col('ICDDiag8'),
                                       f.col('ICDDiag9'),
                                       f.col('ICDDiag10'),
                                       f.col('ICDDiag11'),
                                       f.col('ICDDiag12'),
                                       f.col('ICDDiag13'),
                                       f.col('ICDDiag14'),
                                       f.col('ICDDiag15'),
                                       f.col('ICDDiag16'),
                                       f.col('ICDDiag17'),
                                       f.col('ICDDiag18'),
                                       f.col('ICDDiag19'),
                                       f.col('ICDDiag20')
                                           ))\
            .select('MemberID',
                    'ClaimID',
                    'ICDVersion',
                    'FROMDATE',
                    'ICDS')
        df = df.withColumn('CODE', f.explode('ICDS')).drop('ICDS')\
           .withColumn('CODE_SYSTEM', f.when(f.col('ICDVersion') == 10, 
                                             'ICD10').otherwise('ICD9'))\
           .withColumn('SUPPLEMENTAL_DATA', f.lit(False))\
           .drop('ICDVersion')\
           .withColumnRenamed('FROMDATE','ONSET_DATE')\
           .withColumn('RESOLVED_DATE', f.lit(''))
        df = df.filter(f.col('CODE').isNotNull())
        return df

    def generate_staging_df(self):
        self.read_input()
        final_df = self.stage_claim_problem_data()
        return final_df




