from pyspark import SparkContext
import sys

if __name__=='__main__':
    sc = SparkContext()
    
    NAICS = set(['452210', '445120', '722410', '722511', '722513', '446110', '446191', '311811', '722515', '445210', '445220', '445230', '445291', '445292', '445299', '445110'])

    
    core_places = sc.textFile('core-places-nyc.csv').filter(lambda x: next(csv.reader([x]))[9] in NAICS).cache()
    weekly_pattern = sc.textFile('weekly-patterns-nyc-2019-2020').filter(lambda x: next(csv.reader([x]))[1] in core_places).cache()
    
    #.saveAsTextFile('output')
