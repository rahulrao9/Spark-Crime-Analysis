#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import types as T
from pyspark.sql.functions import concat
import sys

case_2012_path, case_2013_path, case_2014_path, cases_state_key_path, judges_clean_path, judge_case_merge_key_path, acts_section_path, output_path = sys.argv[1:]

spark = SparkSession.builder.appName("CrimeAnalysis").getOrCreate()

cases_2012 = spark.read.csv(case_2012_path, header=True, inferSchema=True)
cases_2013 = spark.read.csv(case_2013_path, header=True, inferSchema=True)
cases_2014 = spark.read.csv(case_2014_path, header=True, inferSchema=True)
all_cases = cases_2012.union(cases_2013).union(cases_2014)

cases_state_key = spark.read.csv(cases_state_key_path, header=True, inferSchema=True)

################################################################
# T 1.1
# merge state_code and year to be on the safer side and get the required state names by joining
all_cases = all_cases.withColumn("yr_state_code", concat(all_cases["year"], all_cases["state_code"]))
all_cases = all_cases.na.drop(subset=["year", "state_code"])

cases_state_key = cases_state_key.withColumn("yr_state_code", concat(cases_state_key["year"], cases_state_key["state_code"]))

cases_with_states = all_cases.join(cases_state_key, on="yr_state_code")

state_crime_counts = cases_with_states.groupBy("state_name").count()

sorted_states = state_crime_counts.orderBy(col("count").desc())

#top 10 states with the highest crime rates
top_10_states = sorted_states.head(10)
top_10_states_df = spark.createDataFrame(top_10_states)


################################################################
# T 1.2
# Read judges data and judge-case merge data
judges_clean = spark.read.csv(judges_clean_path, header=True, inferSchema=True)
judge_case_merge_key = spark.read.csv(judge_case_merge_key_path, header=True, inferSchema=True)
acts_section = spark.read.csv(acts_section_path, header=True, inferSchema=True)

# select rows with criminal case and count
all_cases = all_cases.na.drop(subset=["ddl_case_id"])
all_cases = all_cases.join(acts_section.select("ddl_case_id", "criminal"), "ddl_case_id", "left")
all_cases = all_cases.filter(all_cases["criminal"] == 1).select("ddl_case_id", "judge_position")
criminal_cases = all_cases.select("ddl_case_id", "judge_position")#.filter(col("judge_position").like("%criminal cases%"))

judges_clean = judges_clean.na.drop(subset=["ddl_judge_id"])
criminal_judges = judges_clean.select("ddl_judge_id", "judge_position")#.filter(col("judge_position").like("%criminal cases%"))

judge_case_merge_key = judge_case_merge_key.na.drop(subset = ["ddl_decision_judge_id", 'ddl_case_id'])
judge_case_merge_key = judge_case_merge_key.join(criminal_judges, judge_case_merge_key["ddl_decision_judge_id"] == criminal_judges["ddl_judge_id"], "inner")
criminal_judgeids_clean = judge_case_merge_key.join(criminal_cases, "ddl_case_id", "inner")

judge_case_counts = criminal_judgeids_clean.groupBy("ddl_decision_judge_id").count()

#sort and gigachad criminal judge 
judge_id = judge_case_counts.orderBy(col("count").desc()).first()["ddl_decision_judge_id"]

#tuple output
final_output = (top_10_states_df.select("state_name").rdd.flatMap(lambda x: x).collect(), int(judge_id))

with open(output_path, "w") as f:
    f.write(f"{final_output}")

spark.stop()

