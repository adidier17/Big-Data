
from pyspark import SparkConf, SparkContext;
from pyspark.mllib.linalg import Vectors;
from pyspark.mllib.stat import Statistics;
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD;
from pyspark.mllib.tree import RandomForest;
from pyspark.mllib.util import MLUtils;
import csv;
import numpy as np;
import datetime as dt;
from dateutil import relativedelta as rd;
import math;

if __name__ == '__main__':
    sc = SparkContext( );

    crimes = sc.textFile( 'hdfs://wolf.iems.northwestern.edu/user/asubramaniam/sparkhw/crimes.csv' );
    extdata =  sc.textFile( 'hdfs://wolf.iems.northwestern.edu/user/asubramaniam/sparkhw/External_Data.csv' );


    # Grab the header from the data
    crimesHeader = crimes.first();
    crimes = crimes.filter(lambda x:x != crimesHeader)
    crimes = crimes.map(lambda x: x.split(","))
    extdataHeader = extdata.first();
    extdata = extdata.filter(lambda x:x != extdataHeader)
    extdata = extdata.map( lambda line: [ float( i ) for i in line.split( ) ] )
    extdata = extdata.filter(lambda x: float(x[2]) > 2000 and float(x[2]) < 2016)
    extdata = extdata.map(lambda x: (dt.date(x[2],x[0], x[1]),x[3]));


    beatData = crimes.filter(lambda x: x[10].isdigit()).map(lambda x: (x[10],dt.datetime.strptime(x[2], '%m/%d/%Y %I:%M:%S %p' )))

    
    # Obtain the current year
    latestYear = beatData.map( lambda x: x[ 1 ].year ).max( );
    
    beats = beatData.map( lambda x: x[ 0 ] ).distinct( ).zipWithIndex( ).cache( );
    
    # Mapping each beat to its index
    beatsDict = dict( beats.collect( ) );
    
    # Compute the number of weekly crime events for each beat between 2001 and 2015
    # beat,year,week
    weeklycrimeCounts = beatData.filter( lambda x: x[ 1 ].year < latestYear ).map( lambda x: ( ( beatsDict[ x[ 0 ] ], x[ 1].year, x[ 1 ].isocalendar( )[ 1 ] - 1 ), 1 ) ) \
                        .reduceByKey( lambda x, y: x + y ).cache( );
    
    beatData.unpersist();
    # Obtain all year-week combinations in the dataset
    yearWeeks = weeklycrimeCounts.map( lambda x: ( x[ 0 ][ 1 ], x[ 0 ][ 2 ] ) ).distinct( );
    # Generate all possible beat-year-week combinations from 2001 to 2014
    BeatYearWeeks = beats.values( ).cartesian( yearWeeks ).map( lambda x: ( x[ 0 ], x[ 1 ][ 0 ], x[ 1 ][ 1 ] ) );
    
    # Determine missing beat-year-week combinations in the dataset and insert them with
    # count 0 to the crime counts
    missingBeatYearWeeks = BeatYearWeeks.subtract( weeklycrimeCounts.keys( ) );
    allCrimeCounts = weeklycrimeCounts.union( missingBeatYearWeeks.map( lambda x: ( x, 0 ) ) );
    
      
    # Compute the average weekly temperature for each year
    avgTemperature = extdata.map( lambda x: ( ( x[ 0 ].year, x[ 0 ].isocalendar( )[ 1 ] ), ( x[ 1 ], 1 ) ) ).reduceByKey( lambda x, y: ( x[ 0 ] + y[ 0 ], x[ 1 ] + y[ 1 ] ) )\
                            .mapValues( lambda x: x[ 0 ] / x[ 1 ] );
    
    # Join the crime counts and average weekly temperature datasets, using year-week as key,
    # unnest each row to a flat list, drop the year variable and convert to a LabeledPoint object
    joinedData = allCrimeCounts.map( lambda x: ( ( x[ 0 ][ 1 ], x[ 0 ][ 2 ] ), ( x[ 0 ][ 0 ], x[ 1 ] ) ) ).join( avgTemperature )\
                               .map( lambda x: [ item for sublist in x for item in sublist ] ) \
                               .map( lambda x: LabeledPoint( x[ 2 ][ 1 ], [ x[ 2 ][ 0 ], x[ 1 ], x[ 3 ] ] ) ).cache( );
    
    weeklycrimeCounts.unpersist();
    # Split the crime counts into training and test datasets
    ( training, test ) = joinedData.randomSplit( ( 0.7, 0.3 ) );
    
    # Categorical features dictionary
    featuresInfo = { 0: len( beatsDict ), 1: 53 };
    
    # Train a Random Forest model to predict crimes
    model = RandomForest.trainRegressor( training, categoricalFeaturesInfo = featuresInfo,
                                         numTrees = 20, featureSubsetStrategy = "auto",
                                         impurity = 'variance', maxDepth = 10, maxBins = len( beatsDict ) );
    
    # Measure the model performance on test dataset
    prediction = model.predict( test.map( lambda x: x.features ) ); 

    
    meanCrimes = test.map( lambda x: x.label ).mean( );
    labelsAndPredictions = test.map( lambda x:  x.label ).zip( prediction );
    testMSE = labelsAndPredictions.map( lambda ( actual, predicted ): ( actual - predicted ) * ( actual - predicted ) ).sum( ) / float( test.count( ) );
    testSSE = labelsAndPredictions.map( lambda ( actual, predicted ): ( actual - predicted ) * ( actual - predicted ) ).sum( );
    testSST = labelsAndPredictions.map( lambda ( actual, predicted ): ( actual - meanCrimes ) * ( actual - meanCrimes ) ).sum( );
    
    Rsquared = 1 - testSSE / testSST;
    
    #### Predicting crimes for next week ####
    weekNum = 27;
    tempForecast = [ 65.0, 71, 68.0, 70.0, 71.0, 71.0, 76.5 ];
    
    # Average temperature for next week
    tempNextWeek = sum( tempForecast ) / len( tempForecast );
    
    # Inverse beats dictionary to map index to beat number
    beatsDictInverse = dict( ( val, key ) for key, val in beatsDict.items( ) );
    
    # Test dataset for each beat with next week's info
    nextWeek = sc.parallelize( tuple( [ ( beat, weekNum, tempNextWeek ) for beat in range( len( beatsDict ) ) ] ) );
    predictionsNextWeek = model.predict( nextWeek ).zip( nextWeek.map( lambda x: beatsDictInverse[ x[ 0 ] ] ) ).sortByKey( False );
    
    # Obtain the top 10 beats with highest likelihood of crime
    topCrimeBeats = predictionsNextWeek.take( 10 );
    
    sc.stop( );
    
    print( 'Test Mean Squared Error = ' + str( testMSE ) );
    print( 'Test R-squared = ' + str( Rsquared ) );
    print( topCrimeBeats );
