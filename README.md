# iembd_hadoop
Projects for Hadoop class in Master in Big Data and Business Analytics in IE HST.


# Querying file with twitter data
Task:
The dataset contains social media sentiments for cryptocurrencies. It is a tab delimited file containing english words (in lower case) with their
sentiment polarity. It has the following columns:  
type:string  
length:integer  
word:string  
word_type:string  
stemmed:string  
polarity:string   

Query the following information:

1. create a database named crypto_team_c .  
2. select the database you just created so that all the tables you are going to
create belong to that database.  
3. create an external table named sentiment_dictionary over the file located in
/user/flume/sentiment-dictionary/ .  
4. create an external table named tweets_json over the files located in
/user/flume/crypto-tweets/ .  
You don’t need to reference all the fields in a tweet, just the ones to solve
your assignment.  
You can use the table definition we saw during hive lab as a template (you will
have to add and remove some fields).  
5. write a query that returns the total number of tweets in table tweets_json .
Annotate both the number of records and the amount of seconds that it took.  
6. create a managed table tweets_orc with same schema as tweets_json but
stored in orc format.  
(hint: create table … like)
7. insert all rows from tweets_json into tweets_orc .  
(hint: insert into ...)
8. write a query that returns the total number of tweets in table tweets_orc .
Annotate both the number of records and the amount of seconds that it took.  
9. verify that both tables contain the same number of tweets.
which of the queries was faster?  
10. write a query that returns the total number of users with geolocation enabled
from table tweets_orc .  
11. write a query that returns the total number of tweets per language from table
tweets_orc .  
12. write a query that returns the top 10 users with more followers from table
tweets_orc .  
13. write a query that returns the geoname latitude, longitude and timezone of the
tweet place by joining geonames and tweets_orc .  
(hint: there can be places with the same name in different countries
normalize (upper/lower case) the place name when joining)
14. write a query that returns the total count, total distinct count, maximum,
minimum, average, standard deviation and percentiles 25th, 50th, 75th, 100th
of user mentions in tweets from table tweets_orc  
15. write a query that returns the top 10 users more mentioned from table
tweets_orc  
16. create a table tweet_words in parquet format exploding the words in the
tweets. Also normalize the words to lower case.
(hint: use lateral view)  
17. create a table tweet_words_sentiment in parquet format as the result of a
query that returns the polarity of each word by left joining tweet_words with
sentiment_dictionary. The polarity for non joining words will be neutral (you
can use coalesce function). Also codify the polarity (you can use case when
…) as integer in the following way:  
positive ->1  
neutral -> 0  
negative -> -1  
