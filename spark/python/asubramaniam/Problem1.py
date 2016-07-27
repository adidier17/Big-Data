
from pyspark import SparkConf, SparkContext;
import pyspark.sql as sql;
from pyspark.sql import SQLContext, Row;
import csv;

if __name__ == '__main__':
    sc = SparkContext( );
    sqlContext = sql.SQLContext( sc );

    crimes = sc.textFile( 'hdfs://wolf.iems.northwestern.edu/user/asubramaniam/sparkhw/crimes.csv' );

    # Grab the header from the data
    crimesHeader = crimes.first();
    crimes = crimes.filter(lambda x:x != crimesHeader) 
    crimes = crimes.map(lambda x: x.split(",")) 
    crimeRows = crimes.map(lambda p: Row(ID = p[0], Month = p[2].split(" ")[0].split("/")[0], Year = p[2].split(" ")[0].split("/")[2]))

    schema_crimes = sqlContext.inferSchema(crimeRows)
    schema_crimes.registerTempTable('crimestable')

    # Query to fetch average crimes for each month across all years
    result = sqlContext.sql( 'select Month, count( ID ) / count( distinct Year) as average_crime_count ' \
                             'from crimestable ' \
                             'group by Month' );
    counts = result.map( lambda row: ( row.Month, row.average_crime_count ) );

    for c in counts.collect( ):
        print 'Month', c[ 0 ], '\tCount', c[ 1 ];

    sc.stop( );
