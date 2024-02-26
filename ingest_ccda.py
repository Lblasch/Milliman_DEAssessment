import os
import argparse
from lxml import etree
import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def process_problem(file):
    MemberID = filename.split('_')[0]
    DocumentID = filename.split('_')[1]
    tree = etree.parse(file)
    xpath_value_icd = '//*[contains(@codeSystemName, "ICD10-CM")]'
    # Problem List
    icds = tree.xpath(xpath_value_icd)
    icdcodes=[]
    array=[]
    for element in icds:
        codeval = element.get('code')
        code_sys = element.get('codeSystemName')
        array = [codeval, code_sys, MemberID, DocumentID]
        icdcodes.append(array)
    return icdcodes

def process_medication(file):
    MemberID = filename.split('_')[0]
    DocumentID = filename.split('_')[1]
    tree = etree.parse(file)
    xpath_value_medication = '//*[contains(@codeSystemName, "RxNorm")]'
    meds= tree.xpath(xpath_value_medication)
    rxnormcodes=[]
    array_meds=[]
    for element in meds:
        code_med = element.get('code')
        code_sys_med = element.get('codeSystemName')
        array_meds = [code_med, code_sys_med, MemberID, DocumentID]
    rxnormcodes.append(array_meds)
    return rxnormcodes


def transform_to_df(list):
    # FUTURE - Update to parse all of the relevant fields instead of using placeholders
    # Handle empty lists
    list = [sublist for sublist in list if sublist]
    cols = ['code', 'code_system', 'MemberID', 'DocumentID']
    df = spark.createDataFrame(list).toDF(*cols)
    df= df.withColumn('SUPPLEMENTAL_DATA',f.lit(True))\
          .withColumn('ONSET_DATE',f.lit('2020-01-01').cast('timestamp'))\
          .withColumn('RESOLVED_DATE',f.lit('2030-01-01').cast('timestamp')).where(f.col('code')!='')
    df= df.select('MemberID',
                  'DocumentID',
                  'ONSET_DATE',
                  'CODE',
                  'CODE_SYSTEM',
                  'SUPPLEMENTAL_DATA',
                  'RESOLVED_DATE')\
          .withColumnRenamed('DocumentID','ClaimID')
    return df

def transform_to_df_med(list):
    # FUTURE - Update to parse all of the relevant fields instead of using placeholders
    cols = ['code', 'code_system', 'MemberID', 'DocumentID']
    # Handle empty lists
    list = [sublist for sublist in list if sublist]
    df = spark.createDataFrame(list).toDF(*cols)
    df= df.withColumn('SUPPLEMENTAL_DATA',f.lit(True)).\
        withColumn('DaysSupply',f.lit(0)).\
        withColumn('QuantityDispensed',f.lit(0))
    df = df.select('MemberID',
                   'DocumentID',
                   'DaysSupply',
                   'QuantityDispensed',
                   'SUPPLEMENTAL_DATA',
                   'CODE',
                   'CODE_SYSTEM')\
            .withColumnRenamed('DocumentID','ClaimID')
    return df



def argparser(argv=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "-d", "--directory", help="url directory", type=str, required=True )
    parser.add_argument(
        "-o", "--output_folder", help="output folder", type=str, required=True )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = argparser()
    
    spark = SparkSession.builder.getOrCreate()
    spark = SparkSession.builder \
    .master("local[1]") \
    .appName("spark") \
    .getOrCreate()

    directory = os.fsencode(args.directory)
    icddata = []
    meddata = []
    
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        full_path = '/mnt/c/Users/Lauren/Desktop/Lauren/Milliman/data/raw/CCDA_downloads/' + filename
        if filename.endswith("_masked.xml") :
            icd_list = process_problem(full_path)
            icddata.extend(icd_list)
            med_list = process_medication(full_path)
            meddata.extend(med_list)
            continue
        else:
            continue

icd_final_path = args.output_folder + 'staging_ccda_diagnosis.csv'
med_final_path = args.output_folder + 'staging_ccda_med.csv'

icd_final_df = transform_to_df(icddata)
icd_final_df.coalesce(1).write.mode("overwrite").option("header","true").csv(icd_final_path)

med_final_df = transform_to_df_med(meddata)
med_final_df.coalesce(1).write.mode("overwrite").option("header","true").csv(med_final_path)


