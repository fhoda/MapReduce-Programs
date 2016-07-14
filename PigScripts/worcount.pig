lines = LOAD '/user/cloudera/wordcount/input/nytimes.txt' AS (line:chararray);

words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) as word;

grouping = GROUP words BY word;

count = FOREACH grouping GENERATE group, COUNT(words);

STORE  count INTO 'user/cloudera/wordcount/output/results.txt';