from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.stat import Statistics
import numpy as np
from datetime import datetime, date
import math

if __name__ == '__main__':
    sc = SparkContext( );

    crimes = sc.textFile( 'hdfs://wolf.iems.northwestern.edu/user/asubramaniam/sparkhw/crimes.csv' );

    # Grab the header from the data
    crimesHeader = crimes.first();
    crimes = crimes.filter(lambda x:x != crimesHeader)
    crimes = crimes.map(lambda x: x.split(","))
    
    # Get the block and year for each row and retain only those crimes that occurred after 2012
    crimesLast3Years = crimes.map( lambda row: ( row[3],  row[2].split(" ")[0].split("/")[2] ) ).filter( lambda row: int(row[1]) > 2012 );

    
    # Count all crimes per block in the last 3 years
    blockCrimes = crimesLast3Years.map( lambda row: ( row[0], 1 ) ).reduceByKey( lambda x, y: x + y );

    #get top 10 counts
    sortedblockCrimes = blockCrimes.takeOrdered(10, key = lambda x: -x[1])

    
    
    for pair in sortedblockCrimes:
        print "Block:", pair[ 0 ], "\tCrime Count:", pair[ 1 ];



    #get the counts of crimes by beat and month and year
    beatCrimes = crimes.map( lambda row: ( (row[10], row[2].split(" ")[0].split("/")[0] + '-' + row[2].split(" ")[0].split("/")[2]),1)).reduceByKey( lambda x,y: x + y );

    #get all possible combinations of beat and month and year
    beats = beatCrimes.map(lambda x: x[0][0]).distinct()
    monthYear = beatCrimes.map(lambda x: x[0][1]).distinct()
    beatsMonthsYears = beats.cartesian(monthYear).map(lambda x: (x,0))
    #merge
    merged = beatsMonthsYears.union(beatCrimes).map(lambda x: (x[0],x[1]))
    merged_final = merged.reduceByKey(lambda x,y : int(x) + int(y))

    #sort by month-year
    # Map each year to all beats and their corresponding crime counts for that year, and sort the counts 
    # by beat
    groupedbeatCountsbymonthyear = merged_final.map( lambda row: ( row[ 0 ][ 1 ], ( row[ 0 ][ 0 ], row[ 1 ] ) ) ) \
                                   .groupByKey( ) \
                                   .mapValues( lambda val: sorted( list( val ), key = lambda t: t[ 0 ] ) );
    # Create a list of all beats
    groupbeats = [ elem[ 0 ] for elem in groupedbeatCountsbymonthyear.values( ).first( ) ];
    
    beatvectorCounts = groupedbeatCountsbymonthyear.values( ) \
                                .map( lambda row: Vectors.dense( [ elem[ 1 ] for elem in row ] ) );
    
    # Compute correlation between all beats for yearly crime counts
    corrMatrix = Statistics.corr( beatvectorCounts, method = 'pearson' );
    
     # Fill the diagonal of correlation matrix with 0's
    corrMatrix.flags[ 'WRITEABLE' ] = True;
    np.fill_diagonal( corrMatrix, 0.0 );

    # Get the 10 largest correlation values from the matrixr The correlation matrix is symmetric so
    # we take the largest 20 and step by 2. Finally, the index of the corresponding beat pairs for
    # top 10 correlation values is obtained.
    sortOrder = corrMatrix.argsort( axis = None );
    indices = np.unravel_index( sortOrder[ -20::2 ], corrMatrix.shape  );

    # The corresponding beats names are obtained for the top 10 correlated beat pairs
    topBeatPairs = [ ( groupbeats[ i ], groupbeats[ j ] ) for i, j in zip( indices[ 0 ], indices[ 1 ] ) ];

    for i, j in topBeatPairs:
        print i, j;

     # The corresponding correlations are obtained for the top 10 correlated beat pairs
    topbeatcorrelations = [ ( corrMatrix[i][j] ) for i, j in zip( indices[ 0 ], indices[ 1 ] ) ];
    print topbeatcorrelations


    # select relevant columns 2: date,11: District (key = district, value=date)
    mayorcrimes = crimes.map(lambda p: (p[11],datetime.strptime(p[2],'%m/%d/%Y %I:%M:%S %p').date())).filter(lambda x: x[0].isdigit())
    # Create separate RDD's for Daly and Emanuel
    daly = mayorcrimes.filter(lambda x: x[1] < date(2011, 05, 16))
    emanuel  = mayorcrimes.filter(lambda x: x[1] >= date(2011,05,16))

    d_pairs = daly.map(lambda x: (x[0],1)) # key is district       
    d_counts = d_pairs.reduceByKey(lambda x,y: int(x) + int(y))
    d_months = daly.map(lambda x: x[1].strftime('%Y-%m')).distinct().count() # months Daley in data

    e_pairs = emanuel.map(lambda x: (x[0],1)) # key is district         
    e_counts = e_pairs.reduceByKey(lambda x,y: int(x) + int(y))
    e_months = emanuel.map(lambda x: x[1].strftime('%Y-%m')).distinct().count() # months Emanuel in data

    districtCrimes_Daly = daly.map( lambda x: ( x[ 0 ], 1 ) ) \
                                     .reduceByKey( lambda x, y: x + y ) \
                                     .filter( lambda x: x[ 0 ] > 100 ) \
                                     .map( lambda x: ( x[ 0 ], x[ 1 ] / d_months/12 ) );
    districtCrimes_Rahm  = emanuel.map(  lambda x: (x[ 0 ], 1 ) ) \
                                    .reduceByKey(  lambda x, y: x + y ) \
                                    .filter( lambda x: x[ 0 ] > 100 ) \
                                    .map(  lambda x: (x[ 0 ], x[ 1 ] / e_months/12  ) );

    # Join the average crime per district for both mayors and compute the difference
    joinedDistrictCrimes = districtCrimes_Daly.join( districtCrimes_Rahm );
    deltaDistrictCrimes = joinedDistrictCrimes.map( lambda x: x[ 1 ][ 0 ] - x[ 1 ][ 1 ] )
                                              

    # Perform a paired t-test and compute the t-score
    meanDiff = deltaDistrictCrimes.mean( );
    sdDiff = deltaDistrictCrimes.sampleStdev( );
    n = deltaDistrictCrimes.count( );
    tstat = meanDiff / ( sdDiff / math.sqrt( n ) );
    
    sc.stop( );
    print 'Difference Mean:\t', meanDiff, '\nDifference Std Dev:\t', sdDiff, '\nN:\t\t\t', n, '\nT-statistic:\t\t', tstat;

 
