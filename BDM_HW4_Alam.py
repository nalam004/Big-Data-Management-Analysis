from pyspark import SparkContext
import sys
from datetime import datetime as dt, timedelta as td
import ast

if __name__=='__main__':
    sc = SparkContext()
    
    def getVisits(x):
      dic = {}
      visits = []
      lis = ast.literal_eval(x[3])
      start = x[1].split('T')[0]
      end = x[2].split('T')[0]

      sd = dt.strptime(start,'%Y-%m-%d')
      ed = dt.strptime(end,'%Y-%m-%d')
      filtered_date = dt.strptime('2020-03-17','%Y-%m-%d')
      delta = ed - sd

      for i in range(delta.days):
        dic[sd + td(days = i)] = lis[i]

      for key, value in dic.items():
        if (key >= filtered_date):
          visits.append(value)

      return (x[0], visits)
    
    big_box_grocers = sc.textFile('core-places-nyc.csv').filter(lambda x: next(csv.reader([x]))[9] in set(['452210'])).cache()
    convenience_stores = sc.textFile('core-places-nyc.csv').filter(lambda x: next(csv.reader([x]))[9] in set(['445120'])).cache() 
    drinking_places = sc.textFile('core-places-nyc.csv').filter(lambda x: next(csv.reader([x]))[9] in set(['722410'])).cache() 
    full_service_restaurants = sc.textFile('core-places-nyc.csv').filter(lambda x: next(csv.reader([x]))[9] in set(['722511'])).cache() 
    limited_service_restaurants = sc.textFile('core-places-nyc.csv').filter(lambda x: next(csv.reader([x]))[9] in set(['722513'])).cache() 
    pharmacies_drug_stores = sc.textFile('core-places-nyc.csv').filter(lambda x: next(csv.reader([x]))[9] in set(['446110', '446191'])).cache()
    snack_bakeries = sc.textFile('core-places-nyc.csv').filter(lambda x: next(csv.reader([x]))[9] in set(['311811', '722515'])).cache()
    specialty_food_stores = sc.textFile('core-places-nyc.csv').filter(lambda x: next(csv.reader([x]))[9] in set(['445210', '445220', '445230', '445291', '445292', '445299'])).cache() 
    supermarkets = sc.textFile('core-places-nyc.csv').filter(lambda x: next(csv.reader([x]))[9] in set(['445110'])).cache()
    
    NAICS = set(['452210', '445120', '722410', '722511', '722513', '446110', '446191', '311811', '722515', '445210', '445220', '445230', '445291', '445292', '445299', '445110']) 
    
    core_places = sc.textFile('core-places-nyc.csv').filter(lambda x: next(csv.reader([x]))[9] in NAICS).cache()
    # filter by safegraph_place_id
    weekly_pattern = sc.textFile('weekly-patterns-nyc-2019-2020').filter(lambda x: next(csv.reader([x]))[1] in core_places[1]).cache()
    
    # safegraph_place_id, "date_range_start", "date_range_end", "visits_by_day", "median_dwell"
    rddA = weekly_pattern.map(lambda x: next(csv.reader([x]))).map(lambda x: (x[1], x[12], x[13], x[16], x[23]))
    
    
    
    
    #.saveAsTextFile('output')
