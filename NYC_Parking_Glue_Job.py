#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
from pyspark.sql import *
from pyspark.sql.functions import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col
from pyspark.sql.functions import lit

args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

job.init(args['JOB_NAME'], args)
bucket = "project777"
inputPath = f"s3://{bucket}/raw/"
outputPath = f"s3://{bucket}/MainTransformation/output/"


def extract(spark, input_path):
    """
    Method will read the data from specified path and returns spark dataframe
    :param spark: spark session object
    :param path: input path of file
    :return: spark dataframe
    """
    df = spark.read.option("inferSchema", "true").option("header", "true").csv(input_path)
    return df


def transform(df, glueContext):
    """
    Methods perfroms initial transformation on source data
    :param df: source dataframe
    :param glueContext: glue context
    :return: Dynamic glue DataFrame
    """
    #Replacing column name space with underscore
    df = df.toDF(*(colName.replace(' ', '_') for colName in df.columns))
                                                                                           
    #Dropping Unneccesary column and creating final dataframe
    df = df.select('Summons_Number','Plate_ID','Registration_State','Issue_Date','Violation_Code','Vehicle_Body_Type','Vehicle_Make','Violation_Precinct','Issuer_Precinct','Street_Name')
    
    #Dropping duplicate summon numbers
    df = df.select('Summons_Number').dropDuplicates()
    
    #Dropping null values from data    
    df = df.na.drop()
    
    #Converting Issued_Date column string datatype to date datatype                                                                                       
    df=df.withColumn("Issue_Date",to_date(col("Issue_Date"),"MM/dd/yyyy"))
    
    #Extracting data for fiscal year 2014-04-01 to 2022-03-31
    df=df.filter(df["Issue_Date"] >= lit('2014-04-01'))        .filter(df["Issue_Date"] <= lit('2022-03-31'))
    
    #Replacihg the state named 99 with NY, as NY has the maximum violations.   
    df = df.withColumn('Registration_State', regexp_replace('Registration_State', '99', 'NY'))
    
    
    #Removing the rows containing value as BLANKPLATE for plate_id 
    df = df[df.Plate_ID != 'BLANKPLATE'] 
    
    #Removing the rows containing value as 999 for plate_id 
    df = df[df.Plate_ID != '999'] 
    
    #Codes other than those between 1 and 99 are invalid so removing rows with 0 as violation code
    df = df[df.Violation_Code != 0 ]
    
    
    #Violating Precinct' and 'Issuing Precinct' having invalid entry So removing from columns
    df = df[df.Violation_Precinct != 0 ]
    
    final = df[df.Issuer_Precinct != 0 ]
    
    return DynamicFrame.fromDF(final, glueContext, "project_assignment")

def load(df, output_path):
    """
    Writes data to s3 on specified path
    :param df: glue Dynamic dataframe
    :param output_path: s3 location to write the data
    :return: None
    """
    datasink = glueContext.write_dynamic_frame_from_options(frame=df, connection_type="s3",
                                                            connection_options={"path": output_path}, format="csv",
                                                            transformation_ctx="datasink")


source_df = extract(spark, inputPath)
dynamic_df = transform(source_df, glueContext)
load(dynamic_df, outputPath)

job.commit()




