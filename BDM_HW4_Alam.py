from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import datetime
import numpy as np
import sys
import ast

def main(sc, spark):
    dfPlaces = spark.read.csv('/data/share/bdm/core-places-nyc.csv', header=True, escape='"')
    dfPattern = spark.read.csv('/data/share/bdm/weekly-patterns-nyc-2019-2020/*', header=True, escape='"')
    OUTPUT_PREFIX = sys.argv[1]

    CAT_CODES = {'445210', '722515', '445299', '445120', '452210', '311811', '722410', '722511', '445220', '445292', '445110', '445291', '445230', 
                 '446191', '446110', '722513', '452311'}
    CAT_GROUP = {'452311': 0, '452210': 0, '445120': 1, '722410': 2, '722511': 3, '722513': 4, '446191': 5, '446110': 5, '722515': 6, '311811': 6, 
                 '445299': 7, '445220': 7, '445292': 7, '445291': 7, '445230': 7, '445210': 7, '445110': 8}

    dfD = dfPlaces.filter(F.col('naics_code').isin(CAT_CODES)).select('placekey', 'naics_code')
    udfToGroup = F.udf(CAT_GROUP.get, T.IntegerType())
    dfE = dfD.withColumn('group', udfToGroup('naics_code'))

    dfF = dfE.drop('naics_code').cache()
    groupCount = dict(dfF.groupBy('group').count().collect())

    def expandVisits(date_range_start, visits_by_day):
        dic = []
        lis = ast.literal_eval(visits_by_day)
        start = datetime.datetime.strptime(date_range_start.split('T')[0],'%Y-%m-%d')
        end = start + datetime.timedelta(days=len(lis))
        delta = end - start

        for i in range(delta.days):
            if (lis[i]==0): continue
            dt = start + datetime.timedelta(days=i)
            if dt.year in (2019, 2020):
                dic.append((dt.year, f'{dt.month:02d}-{dt.day:02d}', lis[i]))
        
        return dic

    def computeStats(group, visits):
        vis = np.fromiter(visits, int)
        vis.resize(groupCount[group])
        med = np.median(vis)
        stdev = np.std(vis)
        return(int(med + 0.5), max(0, int(med - stdev + 0.5)), int(med + stdev + 0.5))

    visitType = T.StructType([T.StructField('year', T.IntegerType()), T.StructField('date', T.StringType()), T.StructField('visits', T.IntegerType())])
    statsType = T.StructType([T.StructField('median', T.IntegerType()), T.StructField('low', T.IntegerType()), T.StructField('high', T.IntegerType())])
    
    udfExpand = F.udf(expandVisits, T.ArrayType(visitType))
    udfComputeStats = F.udf(computeStats, statsType)

    dfH = dfPattern.join(dfF, 'placekey').withColumn('expanded', F.explode(udfExpand('date_range_start', 'visits_by_day'))).select('group', 'expanded.*')

    dfI = dfH.groupBy('group', 'year', 'date').agg(F.collect_list('visits').alias('visits')).withColumn('stats', udfComputeStats('group', 'visits'))

    dfJ = dfI.select('group', 'year', 'date', 'stats.*').orderBy('group', 'year', 'date').withColumn('date', F.concat(F.lit('2020-'), F.col('date'))).cache()

    filename = ['big_box_grocers', 'convenience_stores', 'drinking_places', 'full_service_restaurants',
                'limited_service_restaurants', 'pharmacies_and_drug_stores', 'snack_and_retail_bakeries',
                'specialty_food_stores', 'supermarkets_except_convenience_stores']

    for i in range(len(filename)):
        dfJ.filter(F.col('group')==i).drop('group').coalesce(1).write.csv(f'{OUTPUT_PREFIX}/{filename[i]}', mode='overwrite', header=True)

if __name__=='__main__':
    sc = SparkContext()
    spark = SparkSession(sc)
    main(sc, spark)
