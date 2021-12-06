import csv
import datetime
import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, MapType, StringType
from datetime import datetime as dt, timedelta as td
import ast
import pyspark.sql.functions as func 

if __name__=='__main__':
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)
    
    # returns number of visits for the day
    def expandVisits(date_range_start, visits_by_day):
        dic = {}
        lis = ast.literal_eval(visits_by_day)
        start = date_range_start.split('T')[0]
        sd = dt.strptime(start,'%Y-%m-%d')
        end = sd + datetime.timedelta(days=len(lis))

        delta = end - sd
        for i in range(delta.days):
            dic[(sd + td(days = i)).date()] = lis[i]
            
        return (dic)
    
    # code for each store type
    NAICS = set(['452210', '452311', '445120', '722410', '722511', '722513', '446110', '446191', '311811', '722515', '445210', '445220', '445230', '445291', '445292', '445299', '445110']) 

    # file for only items with one of the 9 store type
    core_places = spark.read.csv('core-places-nyc.csv', header=True, escape='"').where(F.col('naics_code').isin(NAICS))
    weekly_patterns = spark.read.csv('weekly-patterns-nyc-2019-2020', header=True, escape='"')
    
    big_box_grocers = core_places.where(F.col('naics_code').isin(set(['452210'])))
    convenience_stores = core_places.where(F.col('naics_code').isin(set(['445120']))) 
    drinking_places = core_places.where(F.col('naics_code').isin(set(['722410']))) 
    full_service_restaurants = core_places.where(F.col('naics_code').isin(set(['722511']))) 
    limited_service_restaurants = core_places.where(F.col('naics_code').isin(set(['722513']))) 
    pharmacies_drug_stores = core_places.where(F.col('naics_code').isin(set(['446110', '446191'])))
    snack_bakeries = core_places.where(F.col('naics_code').isin(set(['311811', '722515'])))
    specialty_food_stores = core_places.where(F.col('naics_code').isin(set(['445210', '445220', '445230', '445291', '445292', '445299'])))
    supermarkets = core_places.where(F.col('naics_code').isin(set(['445110'])))

    # joins the core_places with weekly_patterns data
    joined = core_places.join(weekly_patterns, core_places.safegraph_place_id == weekly_patterns.safegraph_place_id, "inner")
    
    # shows only code, date_start, and visits by day
    df = joined.select(core_places.safegraph_place_id, 'naics_code', 'date_range_start', 'visits_by_day')
    
    # maps data to expandVisits function
    udfExpand = F.udf(expandVisits, MapType(DateType(), IntegerType()))
    dfB = df.select('naics_code', F.explode(udfExpand('date_range_start', 'visits_by_day')).alias('date', 'visits'))
    
    # find median and high
    median = dfB.groupBy('naics_code', 'date').agg(func.percentile_approx('visits', 0.5).alias("median"))
    high = dfB.groupBy('naics_code', 'date').agg(func.max('visits').alias('high'))

    # joins median and high data
    dfC = median.join(high, median.naics_code == high.naics_code)
    dfD = dfC.select(median.date, 'median', 'high')
 
    
    
    #.saveAsTextFile('output')
