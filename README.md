# Description

Individual assignment practice for the class.  
Using Twitter API I filtered out tweets from Australia containing the word "politics" in them. Tweets were streamed every 10 seconds. Sample outputs produce real-time hashtag count and word count with and without stop words.

## Sample output from the count of hashtags:

***
**Time: 2020-02-11 13:58:40**

(u'#DelhiElection2020', 1)  
(u'#AAPWinningDelhi', 1)  
(u'#BJP', 1)  
(u'#Bernie2020', 1)   
(u'#BJP_Mukt_Bharat', 1)    

**Time: 2020-02-11 13:58:40**

|hashtag|hashtag_count|
| :-  |      -:   |
|#DelhiElection2020|1|
|#BJP_Mukt_Bharat|1|
|#AAPWinningDelhi|1|
|  #BJP|1|
|  #Bernie2020|1|


… after a while the hashtags change
***

**Time: 2020-02-11 14:00:40**

(u'#DelhiElection2020', 1)  
(u'#AAPWinningDelhi', 1)  
(u'#Bernie2020', 1)  
(u'#SmartNews', 1)  
(u'#BJP', 1)  
(u'#onpoli', 1)  
(u'#SAHElaw', 1)  
(u'#renters', 2)  
(u'#lauraloomerforcongress', 1)  
(u'#UnfitToBePresident', 1)  

**Time: 2020-02-11 14:00:40**

|hashtag|hashtag_count|
| :-  |      -:   |
|#renters|2|
|#SAHElaw|1|
|#UnfitToBePresident|1|
|#lauraloomerforco...|1|
| #Bernie2020|1|
|#BJP_Mukt_Bharats|1|
|#DelhiElection2020|1|
|#AAPWinningDelhi|1|
|#SmartNews|1|
| #BJP|1|

The output of the hashtags count can be seen in the complete_log_3.txt file:

(u'#DelhiElection2020', 1)
(u'#AAPWinningDelhi', 1)
(u'#Bernie2020', 1)
(u'#SmartNews', 1)
(u'#BJP', 1)
(u'#onpoli', 1)
(u'#SAHElaw', 1)
(u'#renters', 2)
(u'#lauraloomerforcongress', 1)
(u'#UnfitToBePresident', 1)
(u'#BJP_Mukt_Bharat', 1)
(u'#1118', 1)
(u'Brexit.#not', 1)

## Sample output of the word count without removing stopwords:

As we can see it shows just prepositions which is not really useful. You can access full word count in the file complete_log_words.txt
***
**Time: 2020-02-10 09:46:02**

(u'Toast', 1)  
(u'', 20)  
(u'from', 8)  
(u'when', 6)  
(u'displayed', 1)  
(u'sicko', 1)  
(u'not', 7)  
(u'Lions', 1)  
(u'RT', 49)  
(u'access', 1)  
...  


**Time: 2020-02-10 09:46:02**

|word|word_count|
| :-  |      -:   |
|RT|49|
|the|45|
|to|25|
|  |20|
|in|19|
|is|19|
|of|18|
|and|13|
|-|11|
|a|11|


After removal of stopwords  (full log of words is in the file complete_log_without_stop_5.txt).

**Time: 2020-02-11 13:35:50**
***
(u'', 30)
(u'lemmings', 1)
(u'jason', 2)
(u'reminded', 1)
(u'matter.', 1)
(u'laura', 1)
(u'sweeping', 1)
(u'@markjohnstonld:', 1)
(u'shot', 1)
(u'https://t.co/tsiyq32jog', 1)
...

** Time: 2020-02-11 13:35:50**

|word|word_count|
| :-  |      -:   |
|rt|49|
| |30|
|politics|13|
|trump|8|
|thank|8|
|@arvindkejriwal:|8|
|ji|6|
|never|5|
|hate|5|
|politics.|5|
