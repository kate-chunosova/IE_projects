-- Question 1
create database crypto_team_c


-- Question 2
use crypto_team_c


-- Question 3
create external table sentiment_dictionary
(type string,
length int,
word string,
word_type string,
stemmed string,
polarity string)
row format delimited fields terminated by '\t'
stored as textfile
location '/user/flume/sentiment-dictionary'


-- Question 4
create external table tweets_json(
created_at string,
id bigint,
id_str string,
text string,
source string,
truncated boolean,
in_reply_to_status_id bigint,
in_reply_to_status_id_str string,
in_reply_to_user_id bigint,
in_reply_to_user_id_str string,
in_reply_to_screen_name string,
`user` struct<
id:bigint,id_str:string,name:string,screen_name:string,location:string,url:string,description:string
,protected:boolean,verified:boolean,followers_count:int,friends_count:int,listed_count:int,favourites_count:int,
statuses_count:int,utc_offset:int,time_zone:string,geo_enabled:boolean,lang:string>,
coordinates STRUCT <coordinates:array<float>>,
place STRUCT <
    country:string,country_code:string,full_name:string,name:string,place_type:string,url:string>,
quoted_status_id bigint,
quoted_status_id_str string,
is_quote_status boolean,
quote_count int,
reply_count int,
retweet_count int,
favorite_count int,
entities struct<hashtags:array<struct<indices:array<int>,text:string>>,user_mentions: array<struct<id:bigint,id_str:string,indices:array<int>,name:string,screen_name:string>>>,
favorited boolean,
retweeted boolean,
possibly_sensitive boolean,
filter_level string,
lang string
)
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
location '/user/flume/crypto-tweets/';


-- Question 5
select count(*) from tweets_json;

-- 24.4 seconds
-- 25446 rows


-- Question 6
create table tweets_orc
  like tweets_json
stored as ORC;


-- Question 7
Insert into tweets_orc select * from tweets_json;


-- Question 8
select count(*) from tweets_orc;

-- 0.046 seconds
-- 25446 records


-- Question 9

-- Both tables have the same N° of rows,
-- orc format was considerably faster:
-- 25 seconds vs 0.046 seconds.


-- Question 10
select count(*)
from tweets_orc
group by `user`.geo_enabled;


--select count(*) from tweets_orc group by `user`.geo_enabled;

--select `user`.geo_enabled, count(*) from tweets_orc group by `user`.geo_enabled;

--select `user`.geo_enabled, count(*) from tweets_orc where `user`.geo_enabled = true group by `user`.geo_enabled;

-- Question 11
select lang, count(*) as TWEETSN
from tweets_orc
group by lang;


-- Question 12
select `user`.screen_name, `user`.followers_count
from tweets_orc
group by `user`.screen_name, `user`.followers_count
order by followers_count desc
limit 10;


-- Question 13
select g.latitude, g.longitude, g.timezone
from geonames g inner join tweets_orc t
on lower(g.name) = lower(t.place.name)
and lower(g.country_code) = lower(t.place.country_code);


-- Question 14
select count(*), count(DISTINCT t1.count_user), min(t1.count_user),
max(t1.count_user), avg(t1.count_user), STDDEV(t1.count_user),
percentile(t1.count_user,0.25),percentile(t1.count_user,0.5),
percentile(t1.count_user,0.75),percentile(t1.count_user,1)
from (select id, count(user_name) as count_user
from tweets_orc
LATERAL VIEW explode(entities.user_mentions.screen_name) adTable as user_name
group by id) as t1;


-- Question 15
select user_name, count(*) as number_mentions
from tweets_orc
LATERAL VIEW explode(entities.user_mentions.screen_name) adTable as user_name
GROUP BY user_name
ORDER BY number_mentions
DESC limit 10;


-- Question 16

create table tweet_words
row format delimited fields terminated by '\t'
stored as parquet
AS select id as id, lower(words) as word
from tweets_orc
LATERAL VIEW explode(split(text,' ')) adTable as words;


-- Question 17
create table tweet_words_sentiment
row format delimited fields terminated by '\t'
stored as parquet
AS select t.id as id, t.word as word, case
when s.polarity in ('negative') then -1
when s.polarity in ('positive') then 1
else 0
end polarity
from tweet_words t left join sentiment_dictionary s on (t.word = s.word);


-- Question 18
create table tweets_sentiment
row format delimited fields terminated by '\t'
stored as parquet
AS select id, case
when sum(coalesce(polarity,0)) > 0 then 'positive'
when sum(coalesce(polarity,0)) < 0 then 'negative'
when sum(coalesce(polarity,0)) = 0 then 'neutral'
end tweet_polarity
from tweet_words_sentiment
group by id;

-- Question 19

select time_interval, sum(coalesce(count_positive)), sum(coalesce(count_negative))
from (select concat(year(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
month(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
day(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
HOUR(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy')))) as time_interval, case
when ts.tweet_polarity= 'positive' then count(ts.id)
end count_positive,
case
when ts.tweet_polarity = 'negative' then count(ts.id)
end count_negative
  from tweets_orc t2
  inner join tweets_sentiment ts on t2.id = ts.id
  inner join (select lower(hashtag1) as hashtag1, id as id2 from tweets_orc
  LATERAL VIEW explode(entities.hashtags.text) adTable as hashtag1) as t3 on (ts.id = t3.id2)
where t3.hashtag1 in ('xrp', 'ripple')
group by concat(year(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
month(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
day(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
HOUR(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy')))), ts.tweet_polarity) as result1
group by result1.time_interval;






--select coordinates.coordinates, count(*) from tweets_orc group by coordinates.coordinates;
--create external table geonames
--(id bigint,
--name string,
--ascii_name string,
--alternate_names array<string>,
--latitude float,
--longitude float,
--feature_class string,
--feature_code string,
--country_code string,
--country_code2 array<string>,
--admin1_code string,
--admin2_code string,
--admin3_code string,
--admin4_code string,
--population bigint,
--elevation int,
--dem int,
--timezone string,
--modification_date date)
--row format delimited
-- fields terminated by '\t'
-- collection items terminated by ','
--stored as textfile
--location '/user/jplarrondo/hive/geonames';

-- select `user`.time_zone, count(*) from tweets_orc group by `user`.time_zone;




-- Trial 1

-- select concat(year(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- month(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- day(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- HOUR(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy')))), case
-- when ts.tweet_polarity= 'positive' then count(ts.id)
-- end count_positive,
-- case
-- when ts.tweet_polarity = 'negative' then count(ts.id)
-- end count_negative
  -- from tweets_orc t2
  -- inner join tweets_sentiment ts on t2.id = ts.id
  -- inner join (select lower(hashtag1) as hashtag1, id as id2 from tweets_orc
  -- LATERAL VIEW explode(entities.hashtags.text) adTable as hashtag1) as t3 on (ts.id = t3.id2)
-- where t3.hashtag1 in ('xrp', 'ripple')
-- group by concat(year(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- month(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- day(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- HOUR(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy')))), ts.tweet_polarity;

--Trial 2

-- select concat(year(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- month(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- day(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- HOUR(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy')))), count_positive, count_negative
  -- from tweets_orc t2
  -- inner join tweets_sentiment ts on t2.id = ts.id
  -- inner join (select lower(hashtag1) as hashtag1, id as id2 from tweets_orc
  -- LATERAL VIEW explode(entities.hashtags.text) adTable as hashtag1) as t3 on (ts.id = t3.id2)
-- where t3.hashtag1 in ('xrp', 'ripple')
-- group by concat(year(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- month(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- day(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- HOUR(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy')))), ts.tweet_polarity
-- having case
-- when ts.tweet_polarity= 'positive' then count(ts.id)
-- end count_positive,
-- case
-- when ts.tweet_polarity = 'negative' then count(ts.id)
-- end count_negative;


--Trial 3

-- select concat(year(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- month(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- day(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- HOUR(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy')))), case
-- when ts.tweet_polarity= 'positive' then count(ts.id)
-- end count_positive
  -- from tweets_orc t2
  -- inner join tweets_sentiment ts on t2.id = ts.id
  -- inner join (select lower(hashtag1) as hashtag1, id as id2 from tweets_orc
  -- LATERAL VIEW explode(entities.hashtags.text) adTable as hashtag1) as t3 on (ts.id = t3.id2)
-- where t3.hashtag1 in ('xrp', 'ripple')
-- group by concat(year(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- month(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- day(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- HOUR(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy')))), ts.tweet_polarity, count_positive
-- having count_positive is not null;


-- select concat(year(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') )),
-- month(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') )),
-- day(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') )),
-- HOUR(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') ))), ts.id, ts.tweet_polarity
  -- from tweets_orc t2 inner join tweets_sentiment ts on t2.id = ts.id;

-- select concat(year(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') )),
-- month(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') )),
-- day(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') )),
-- HOUR(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') ))), ts.id, ts.tweet_polarity, t3.1
  -- from tweets_orc t2
  -- inner join tweets_sentiment ts on t2.id = ts.id
  -- inner join (select lower(hashtag1), id as id2 from tweets_orc
  -- LATERAL VIEW explode(entities.hashtags.text) adTable as hashtag1
  -- where hashtag1 in ('xrp','ripple')) as t3 on (ts.id = t3.id2);










--The one that works!!!!!
-- select concat(year(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- month(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- day(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- HOUR(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy')))), count(ts.id)
  -- from tweets_orc t2
  -- inner join tweets_sentiment ts on t2.id = ts.id
  -- inner join (select lower(hashtag1) as hashtag1, id as id2 from tweets_orc
  -- LATERAL VIEW explode(entities.hashtags.text) adTable as hashtag1) as t3 on (ts.id = t3.id2)
-- where t3.hashtag1 in ('xrp', 'ripple')
-- group by concat(year(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- month(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- day(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))),
-- HOUR(from_unixtime(unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy'))));
