REGISTER 'mr-app/target/pig-1.0-SNAPSHOT.jar';

data = LOAD 'pig/input/input' USING PigStorage (',') AS (jobid: chararray, jobdesc: chararray );
--small sample of job ids
data15 = sample data 0.0003;

stopwords = LOAD 'pig/input/stopwords-en.txt' AS (stopword: chararray);
dictionary = LOAD 'pig/input/dictionary.txt' AS (dictionaryword: chararray);

--tokenize
tokens = foreach data15 generate jobid, FLATTEN( TOKENIZE( LOWER( REPLACE( jobdesc, '([^a-zA-Z0-9\\s\']+)', ' ' ) ) ) ) as jobword;

--remove stopwords
cogroupedtokens = cogroup tokens by jobword, stopwords by stopword;
filteredtokens = filter cogroupedtokens by IsEmpty(stopwords);
filteredtokens = foreach filteredtokens generate flatten( tokens );

--stemming
stemmedtokens = foreach filteredtokens generate jobid, WordStem( jobword ) as jobword;

--check against dictionary
cogrptokens = cogroup stemmedtokens by jobword, dictionary by dictionaryword;
matchedtokens = foreach ( filter cogrptokens by not IsEmpty( dictionary ) ) generate flatten( stemmedtokens );
misspelttokens = foreach( filter cogrptokens by IsEmpty( dictionary ) ) generate flatten( stemmedtokens );

misspelttokensunique = distinct ( foreach misspelttokens generate jobword );
crossedtokens = cross misspelttokensunique, dictionary;
crossedtokensdistance = foreach crossedtokens generate jobword, dictionaryword, CalcLevenshtein( jobword, dictionaryword ) as distance;
tocorrectgroupedtokens = group crossedtokensdistance by jobword;

--get closest words in dictionary
mindisttokens = foreach tocorrectgroupedtokens {
		mindisttoken = order crossedtokensdistance by distance asc;
		mindisttoken = limit mindisttoken 1;
		generate mindisttoken;
	};

dicttokens = foreach mindisttokens generate FLATTEN( mindisttoken.jobword ) as jobword, FLATTEN( mindisttoken.dictionaryword ) as dictword;
correctedtokens = join misspelttokens by jobword, dicttokens by jobword using 'replicated';
correctedtokensfinal = foreach correctedtokens generate jobid, dictword as jobword;

alltokens = union matchedtokens, correctedtokensfinal;
groupedalltokens = group alltokens by jobid;
processedjobtokens = foreach groupedalltokens generate group as jobid, alltokens.jobword as jobword;

rmf processedjobtokens;
store processedjobtokens into 'processedjobtokens' using PigStorage( '\t' );
fs -getmerge processedjobtokens processedjobtokens;
