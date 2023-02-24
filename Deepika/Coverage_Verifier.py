from datetime import date, datetime
import pytz

config = {
    "src_bucket": "s3://va-class1-uw/coverage_verifier/coverage_verifier_raw/dbo",
    "tgt_bucket": "s3://va-class1-uw/coverage_verifier/curated/nfcra",
    "main_table": "cvt00125",
    "tables_list": [
        "cvt00126",
        "cvt00127",
        "cvt00128",
        "cvt00130",
        "cvt00137",
        "cvt00155",
        "cvt00156",
        "cvt00169",
        "cvt00198",
        "cvt00199",
        "cvt001a1",
        "cvt001a2"
    ],
    "years": [
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022"
    ],
    "ambest": {
        "91616": "03-05-2021",
        "93006": "03-05-2021",
        "99005": "03-05-2021",
        "02513": "04-15-2021",
        "03237": "04-27-2021",
        "00277": "05-01-2021",
        "00643": "05-01-2021",
        "01772": "05-01-2021",
        "01872": "05-01-2021",
        "01921": "05-01-2021",
        "01987": "05-01-2021",
        "02014": "05-01-2021",
        "02356": "05-01-2021",
        "02357": "05-01-2021",
        "02358": "05-01-2021",
        "02483": "05-01-2021",
        "02594": "05-01-2021",
        "03539": "05-01-2021",
        "03779": "05-01-2021",
        "10346": "05-01-2021",
        "10709": "05-01-2021",
        "11541": "05-01-2021",
        "11802": "05-01-2021",
        "12121": "05-01-2021",
        "12238": "05-01-2021",
        "80007": "05-01-2021",
        "80009": "05-01-2021",
        "80011": "05-01-2021",
        "80023": "05-01-2021",
        "80024": "05-01-2021",
        "80025": "05-01-2021",
        "80026": "05-01-2021",
        "80027": "05-01-2021",
        "80028": "05-01-2021",
        "80029": "05-01-2021",
        "80030": "05-01-2021",
        "80031": "05-01-2021",
        "80032": "05-01-2021",
        "80033": "05-01-2021",
        "80034": "05-01-2021",
        "80035": "05-01-2021",
        "80036": "05-01-2021",
        "80037": "05-01-2021",
        "80038": "05-01-2021",
        "80039": "05-01-2021",
        "80040": "05-01-2021",
        "80041": "05-01-2021",
        "80043": "05-01-2021",
        "80044": "05-01-2021",
        "80045": "05-01-2021",
        "80046": "05-01-2021",
        "80169": "05-01-2021",
        "80171": "05-01-2021",
        "80173": "05-01-2021",
        "80272": "05-01-2021",
        "80323": "05-01-2021",
        "80358": "05-01-2021",
        "80388": "05-01-2021",
        "80389": "05-01-2021",
        "80415": "05-01-2021",
        "80417": "05-01-2021",
        "80434": "05-01-2021",
        "80435": "05-01-2021",
        "80457": "05-01-2021",
        "80459": "05-01-2021",
        "80500": "05-01-2021",
        "80504": "05-01-2021",
        "80505": "05-01-2021",
        "80510": "05-01-2021",
        "80516": "05-01-2021",
        "90445": "05-01-2021",
        "90450": "05-01-2021",
        "90689": "05-01-2021",
        "91261": "05-01-2021",
        "91271": "05-01-2021",
        "91426": "05-01-2021",
        "91431": "05-01-2021",
        "91621": "05-01-2021",
        "91636": "05-01-2021",
        "91641": "05-01-2021",
        "93086": "05-01-2021",
        "93087": "05-01-2021",
        "93173": "05-01-2021",
        "93253": "05-01-2021",
        "00308": "08-24-2021",
        "00481": "08-24-2021",
        "00864": "08-24-2021",
        "01824": "08-24-2021",
        "02428": "08-24-2021",
        "02429": "08-24-2021",
        "03722": "08-24-2021",
        "03770": "08-24-2021",
        "04207": "08-24-2021",
        "04329": "08-24-2021",
        "04330": "08-24-2021",
        "04406": "08-24-2021",
        "04784": "08-24-2021",
        "00177": "11-12-2021",
        "00270": "11-12-2021",
        "01754": "11-12-2021"
    },
    "ambest_end_date": "2022-12-31"
}


def path_accessible(path):
    """
    Check if the S3 folder exists using databricks dbutils utility
    """
    try:
        dbutils.fs.ls(path)
        return True
    except:
        pass
    return False


# Extracting information from configuration
current_year = date.today().strftime("%Y")
source_bucket = config["src_bucket"]
target_bucket = config["tgt_bucket"]
main_table = config["main_table"]
ambest_dict = config["ambest"]
years_list = config["years"]
tables_list = config["tables_list"]
ambest_end_date = config["ambest_end_date"]
ambest_end_date_formatted = ambest_end_date.replace("-", "")

# Processing for main table
print("=" * 50)
print(f"Table Name: {main_table}")
print(f"Current Date and Time : {datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d:%H:%M:%S')}")

source_count = 0

for year in years_list:
    year_count = 0
    print("=" * 30)
    print(f"Processing Year : {year} ")

    if int(year) <= 2021:
        src_path = f"{source_bucket}/{main_table}/{year}"
        end_date = ambest_end_date
    else:
        src_path = f"{source_bucket}/{main_table}_adl/{year}"
        end_date = ambest_end_date_formatted

    if path_accessible(src_path):
        tgt_path = f"{target_bucket}/{main_table}/year={year}"

        src_df = spark.read.format("parquet").load(src_path)
        src_df.createOrReplaceTempView("src_table")

        # For each amb write data into target
        for ambest_value, start_date in ambest_dict.items():
            start_date = datetime.strptime(start_date, "%m-%d-%Y").strftime("%Y-%m-%d")
            src_df1 = spark.sql(
                f"SELECT * FROM src_table WHERE AMBEST = '{ambest_value}' AND POL_TRM_BEGIN  >= '{start_date}' AND Date_Insert  <= '{end_date}'")
            ambest_count = src_df1.count()
            print(f"Ambest Count for table {main_table} and ambest {ambest_value}: {ambest_count}")
            year_count += ambest_count
            source_count = source_count + ambest_count

            if ambest_count != 0:
                # src_df2 = src_df1.coalesce(100)
                src_df1.write.option("maxRecordsPerFile", 100000).parquet(tgt_path, mode="append")
    else:
        print(f"Path {src_path} does not exists.")

    print(f"Year Count for table {main_table} and year {year}: {year_count}")

print("=" * 30)

print(f"Source count for table {main_table}: {source_count}")
if (source_count > 0):
    print(
        f"""Target count for table {main_table}: {spark.read.format("parquet").load(f"{target_bucket}/{main_table}").count()}""")
else:
    print(f"""Target count for table {main_table}: 0""")

print("=" * 50)

# Processing for remaining tables
for table_name in tables_list:
    print("=" * 50)
    print(f"Table Name: {table_name}")
    print(f"Current Date and Time : {datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d:%H:%M:%S')}")

    source_count = 0

    for year in years_list:
        try:
            year_count = 0
            print("=" * 30)
            print(f"Processing Year : {year} ")

            if int(year) <= 2021:
                main_table_src_path = f"{source_bucket}/{main_table}/{year}"
                current_table_src_path = f"{source_bucket}/{table_name}/{year}"
                end_date = ambest_end_date
            else:
                main_table_src_path = f"{source_bucket}/{main_table}_adl/{year}"
                current_table_src_path = f"{source_bucket}/{table_name}_adl/{year}"
                end_date = ambest_end_date_formatted

            if path_accessible(main_table_src_path) and path_accessible(current_table_src_path):
                tgt_path = f"{target_bucket}/{table_name}/year={year}"

                main_table_df = spark.read.format("parquet").load(main_table_src_path)
                main_table_df.createOrReplaceTempView("main_table")

                current_table_df = spark.read.format("parquet").load(current_table_src_path)
                current_table_df.createOrReplaceTempView("current_table")

                # For each amb write data into target
                for ambest_value, start_date in ambest_dict.items():
                    start_date = datetime.strptime(start_date, "%m-%d-%Y").strftime("%Y-%m-%d")

                    main_table_df1 = spark.sql(
                        f"SELECT distinct I_CV,AMBEST,POLICY FROM main_table WHERE AMBEST = '{ambest_value}' AND POL_TRM_BEGIN >= '{start_date}' AND Date_Insert <= '{end_date}'")
                    main_table_df1.createOrReplaceTempView("filtered_main_table")

                    current_table_df1 = spark.sql(
                        f"SELECT t1.* FROM current_table t1 INNER JOIN filtered_main_table t2 ON  (t1.I_CV = t2.I_CV AND t1.AMBEST = t2.AMBEST AND t1.POLICY = t2.POLICY);")
                    ambest_count = current_table_df1.count()
                    print(f"Ambest Count for table {table_name} and ambest {ambest_value}: {ambest_count}")
                    year_count += ambest_count
                    source_count = source_count + ambest_count

                    if ambest_count != 0:
                        # current_table_df2 = current_table_df1.coalesce(100)
                        current_table_df1.write.option("maxRecordsPerFile", 100000).parquet(tgt_path, mode="append")
            else:
                print(f"Path {main_table_src_path} or {current_table_src_path} does not exists.")

            print(f"Year Count for table {table_name} and year {year}: {year_count}")

        except Exception as e:
            print(f"Error encountered for {table_name}.\nError: {e}")

    print("=" * 30)
    print(f"Source count for table {table_name}: {source_count}")
    if path_accessible(f"{target_bucket}/{table_name}"):
        print(
            f"""Target count for table {table_name}: {spark.read.format("parquet").load(f"{target_bucket}/{table_name}").count()}""")
    else:
        print(f"No data found at {target_bucket}/{table_name}.")
   print("=" * 50)