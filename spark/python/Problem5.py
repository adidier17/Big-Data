
from pyspark import SparkContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.stat import Statistics
import numpy as np
import datetime as dt
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
import csv

if __name__ == '__main__':
    sc = SparkContext( );

    crimes = sc.textFile( 'hdfs://wolf.iems.northwestern.edu/user/asubramaniam/sparkhw/crimes.csv' );
    extdata =  sc.textFile( 'hdfs://wolf.iems.northwestern.edu/user/asubramaniam/sparkhw/Census_Data1.csv' );


    # Grab the header from the data
    crimesHeader = crimes.first();
    crimes = crimes.filter(lambda x:x != crimesHeader)
    crimes = crimes.map(lambda x: x.split(","))
    extdataHeader = extdata.first();
    extdata = extdata.filter(lambda x:x != extdataHeader)
    extdata = extdata.map(lambda x: x.split(","))
    extdata = extdata.map(lambda x: (x[0],x[1]))
   
    
    beatData = crimes.filter(lambda x: x[10].isdigit()).map(lambda x: (dt.datetime.strptime(x[2], '%m/%d/%Y %I:%M:%S %p' ), x[10], x[13]))
    beatData = beatData.filter(lambda x: x[0].year >= 2008 and x[0].year <= 2012)
    # Aggregate the crime count over (year, week, beat)
    beatCount = beatData.map(lambda x: ((x[0].year, x[0].isocalendar()[1], x[1]), 1)).reduceByKey(lambda x,y: x+y)
    
    # List of all the beats
    beatList = beatData.map(lambda x: x[1]).distinct().sortBy(lambda x: x)

    # All year-week combinations in the dataset
    yearWeeks = beatCount.map(lambda x: (x[0][0], x[0][1])).distinct();

    # Get the list of all year-week-beat combinations, and see which combinations are missing in our dataset. 
    # Set those counts equal to 0
    yearWeekBeats = beatList.cartesian(yearWeeks).map(lambda x: (x[1][0], x[1][1], x[0]))
    missingCombo = yearWeekBeats.subtract(beatCount.keys()).distinct()
    overallBeats = beatCount.union(missingCombo.map(lambda x: (x,0)))
    overallBeats.persist()

    beatandCommunity = beatData.map(lambda x: (x[2],x[1]))
    beatandCommunityJoin = beatandCommunity.join(extdata).map(lambda x: (x[1][0], x[1][1]))


    # Join #with the base set
    # baseData RDD: beat, (week, index)
    datatoJoin = overallBeats.map(lambda x: (x[0][2], (x[0][1], x[1])))
    # joinedData RDD: beat, ((week, count, index) => (count, (week, beat, index))
    joinedData = datatoJoin.join(beatandCommunityJoin).map(lambda x: (x[1][0][1], (x[1][0][0], x[0], x[1][1])))

    #for c in joinedData.take(5):
     #   print 'Month', c;


    # Dictionary to map week to an index
    weekDict = dict(zip(range(1,54), range(0,53)))
    # List of all the beats
    # Dictionary mapping each beat to an index. Useful when converting to LabeledPoint. Otherwise converts to numeric.
    beatsDict = dict(beatList.zipWithIndex().map(lambda x: (x[0],x[1])).collect())
    
    # Data points as LabeledPoints
    # (crime count, [beat, week])
    predArrayLP = joinedData.map(lambda x: LabeledPoint(x[0], [weekDict[x[1][0]], beatsDict[x[1][1]], x[1][2]]))

    # Split into training and testing set. 70-30 split.
    (train, test) = predArrayLP.randomSplit([0.7, 0.3])

    # Feature categories : 
    featuresCat = {0: len(beatsDict), 1: 53}
    maxBins = max(len(beatsDict),len(weekDict))

    model = RandomForest.trainRegressor(train, categoricalFeaturesInfo=featuresCat,
                                    numTrees=10, featureSubsetStrategy="auto",
                                    impurity='variance', maxDepth=5, maxBins=maxBins)



    # Evaluate model on test instances and compute test error
    predictions = model.predict(test.map(lambda x: x.features))
    predOutput = predictions.collect()
    labelsAndPredictions = test.map(lambda lp: lp.label).zip(predictions)
    testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() / float(test.count())
    print('Test Mean Squared Error = ' + str(testMSE))

    ### Write output to file ###
    with open("predictions.txt", 'wb') as f:
         writer = csv.writer(f)
         writer.writerows(predOutput)
