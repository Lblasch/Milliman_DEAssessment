# Created a data folder with the following subfolders
/data/raw/ - raw data files
/data/raw/CCDA_downloads - folder with the downloaded  urls
/data/staging/ - after initial transforations
/data/final/ - final data files

The pipeline takes the raw data (claims and ccda) and transforms it into staging. Then the data is merged to the final data folder.

Problems
root
- MemberID: string (nullable = true)
- ClaimID: string (nullable = true)
- CODE: string (nullable = true)
- CODE_SYSTEM: string (nullable = true)
- ONSET_DATE: timestamp (nullable = true)
- RESOLVED_DATE: timestamp (nullable = true)
- SUPPLEMENTAL_DATA: boolean (nullable = true)

Medication
root
- MEMBERID: string (nullable = true)
- CLAIMID: string (nullable = true)
- DaysSupply: integer (nullable = true)
- QuantityDispensed: double (nullable = true)
- CODE: string (nullable = true)
- CODE_SYSTEM: string (nullable = true)
- SUPPLEMENTAL_DATA: boolean (nullable = true)


Run Steps:
1. Run requirements.txt
2. Download urls
3. CCDA to staging - This would be combined to the main script if time allowed
python3 ./Milliman_DEAssessment/ingest_ccda.py -d ./data/raw/CCDA_Downloads/ -o ./data/staging/ 
4. The remaining steps packaged up
python3 ./Milliman_DEAssessment/main.py  -s ./data/staging/ -cl ./data/raw/data_engineer_exam_claims_final.csv -r ./data/raw/data_engineer_exam_rx_final.csv -f ./data/final/


Future Improvements:
- Parse the full ccda files. Initially I only had time to pull a few fields. 
- Add CCDA Staging step as a class and add to main script so it can be run all at once
- Support Incremental updates by adding merge to the stage -> final step
- Write to parquet and add partitioning