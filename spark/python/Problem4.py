from pyspark.sql import SQLContext,Row;
from pyspark import SparkConf, SparkContext;
import datetime as dt;

if __name__ == '__main__':
    sc = SparkContext( );
    sqlContext = SQLContext(sc);
    
    crimes = sc.textFile('hdfs://wolf.iems.northwestern.edu/user/asubramaniam/sparkhw/crimes.csv');
    header = crimes.first();
    crimes_no_header = crimes.filter(lambda x:x !=header);
    
    crimes_arrest = crimes_no_header.map(lambda x : x.split(",")).filter(lambda x: x[8] == 'true');
    
    crimes_date = crimes_arrest.map(lambda x: Row(ID = x[0], month = x[2].split(" ")[0].split("/")[0], DayofWeek = dt.datetime.strptime(x[2].split(" ")[0], '%m/%d/%Y').strftime('%A'), TimeofDay = dt.datetime.strptime(x[2],'%m/%d/%Y %I:%M:%S %p').hour));
    
    schema_file = sqlContext.inferSchema(crimes_date);
    
    schema_file.registerTempTable('arrests_data');
    
    MonthlyPattern = sqlContext.sql('SELECT month, count(*) as monthly_arrests FROM arrests_data group by month');
    for c in MonthlyPattern.collect():
        print c
    #test.rdd.saveAsTextFile('hdfs://wolf.iems.northwestern.edu/user/kbhatt/monthly_crimes');
    WeekdayPattern = sqlContext.sql('SELECT DayofWeek, count(*) as daily_arrests FROM arrests_data group by DayofWeek');
    for c in WeekdayPattern.collect():
        print c
    
    #test.rdd.saveAsTextFile('hdfs://wolf.iems.northwestern.edu/user/kbhatt/weekday_crimes');
    TimeofDayPattern = sqlContext.sql('SELECT TimeofDay, count(*) as hourly_arrests FROM arrests_data group by TimeofDay');
    for c in TimeofDayPattern.collect():
        print c
    #test.rdd.saveAsTextFile('hdfs://wolf.iems.northwestern.edu/user/kbhatt/daytime_crimes');
    sc.stop();


