from pyspark import SparkContext
import sys
from datetime import datetime as dt, timedelta as td
import ast

if __name__=='__main__':
    sc = SparkContext()
    
    # Complete the getVisits function below which takes a record similar to those in rddB, 
    # and return the safegraph_place_id, and visits_by_day filtered to those on or after March 17.
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

    # word count
    weekly_pattern = sc.textFile('weekly-patterns-nyc-2019-2020').cache()
                       .flatMap(lambda x: x.split()) \
                       .map(lambda x: (x,1)) \
                       .reduceByKey(lambda x,y: x+y) \
                       .saveAsTextFile(sys.argv[2] if len(sys.argv)>2 else 'output')
    
    # Complete the transformation to keep only safegraph_place_id, date_range_start, date_range_end, and visits_by_day for each record.
    rddA = weekly_pattern.map(lambda x: next(csv.reader([x]))) \
                         .map(lambda x: (x[1], x[12], x[13], x[16]))
    
    # Transform rddA to keep any record that reported on or after March 17, i.e. as long as the date_range_end is later than March 17.
    rddB = rddA.filter(lambda x: (x[1] > "2020-03-16T00:00:00-00:00" and x[2] > "2020-03-17T00:00:00-00:00"))
    
    # Make use of the getVisits() function above, we can gather all the the visits for each restaurant from rddB.
    rddD = rddB.map(getVisits)
    
    # Transform rddD to compute the maximum number of daily visits for each record.
    rddE = rddD.mapValues(lambda x: max(x))
    
    # Reduce the records in rddE so that for each restaurant id we only have a single maximum number of daily visits (i.e. the max of all values in rddE for each id)
    rddF = rddE.reduceByKey(max)
