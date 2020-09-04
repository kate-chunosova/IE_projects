import os
import re
import datetime

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import functions as sf


month_map = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7,
             'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}


def parse_apache_time(s):

    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


# A regular expression pattern to extract fields from the log line
APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*) ?" (\d{3}) (\S+)'


def parse_apache_log_line(logline):

    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return logline, 0
    size_field = match.group(9)
    if size_field == '-':
        size = long(0)
    else:
        size = long(match.group(9))
    return (Row(
        host=match.group(1),
        client_identd=match.group(2),
        user_id=match.group(3),
        date_time=parse_apache_time(match.group(4)),
        method=match.group(5),
        endpoint=match.group(6),
        protocol=match.group(7),
        response_code=int(match.group(8)),
        content_size=size
    ), 1)


def parse_logs():

    """ Read and parse log file """
    log_file = os.path.join('data', 'apache.access.log')
    parsed_logs_l = (sc
                     .textFile(log_file)
                     .map(parse_apache_log_line))

    access_logs_l = (parsed_logs_l
                     .filter(lambda s: s[1] == 1)
                     .map(lambda s: s[0]))

    failed_logs_l = (parsed_logs_l
                     .filter(lambda s: s[1] == 0)
                     .map(lambda s: s[0]))

    failed_logs_count = failed_logs_l.count()
    if failed_logs_count > 0:
        print 'Number of invalid logline: %d' % failed_logs_l.count()
        for line in failed_logs_l.take(20):
            print 'Invalid logline: %s' % line

    print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs_l.count(),
                                                                                     access_logs_l.count(),
                                                                                     failed_logs_l.count())

    return parsed_logs_l, access_logs_l, failed_logs_l


sc = SparkContext(appName="group_C_sql")
parsed_logs, access_logs, failed_logs = parse_logs()
sqlContext = SQLContext(sc)

# Create a DataFrame from RDD
df = sqlContext.createDataFrame(access_logs)

# Register as a table
df.registerTempTable("AccessLogs")

# question 1: min, max, mean of response size
content_size_stats = sqlContext.sql(
    "SELECT MIN(content_size) AS min, MAX(content_size) AS max, AVG(content_size) AS mean"
    " FROM AccessLogs"
)
print "Q1. Content size: "
content_size_stats.show()

# question 2: number of requests for each response code
response_codes = sqlContext.sql(
    "SELECT response_code, COUNT(*) AS requests"
    " FROM AccessLogs"
    " GROUP BY response_code"
    " ORDER BY requests DESC"
)
print "Q2. Number of requests for each response code: "
response_codes.show()

# question 3: top 20 hosts that have been visited more than 10 times
frequent_hosts = sqlContext.sql(
    "SELECT host, COUNT(*) AS visits"
    " FROM AccessLogs"
    " GROUP BY host"
    " HAVING visits > 10"
    " ORDER BY visits DESC"
    " LIMIT 20"
)
print "Q3. Top 20 hosts that have been visited more than 10 times: "
frequent_hosts.show()

# question 4: top 10 most visited endpoints
frequent_endpoints = sqlContext.sql(
    "SELECT endpoint, COUNT(*) AS visits"
    " FROM AccessLogs"
    " GROUP BY endpoint"
    " ORDER BY visits DESC"
    " LIMIT 10"
)
print "Q4. Top 10 most visited endpoints: "
frequent_endpoints.show()

# question 5: top 10 failed endpoints (response code not equal to 200)
failed_endpoints = sqlContext.sql(
    "SELECT endpoint, COUNT(*) AS failed_visits"
    " FROM AccessLogs"
    " WHERE response_code != 200"
    " GROUP BY endpoint"
    " ORDER BY failed_visits DESC"
    " LIMIT 10"
)
print "Q5. Top 10 failed endpoints: "
failed_endpoints.show()

# question 6: unique hosts
unique_hosts = sqlContext.sql(
    "SELECT COUNT(DISTINCT(host)) AS unique_host"
    " FROM AccessLogs"
)
print "Q6. Unique hosts: "
unique_hosts.show()

# question 7: unique hosts per day
unique_hosts_per_day = sqlContext.sql(
    "SELECT CAST(date_time as DATE) AS day, COUNT(DISTINCT(host)) AS unique_host"
    " FROM AccessLogs"
    " GROUP BY CAST(date_time as DATE)"
)
print "Q7. Number of unique hosts per day: "
unique_hosts_per_day.show()

# question 8: average daily requests per host
avg_daily_requests_per_host = sqlContext.sql(
    "SELECT CAST(date_time as DATE) AS day, COUNT(*) / COUNT(DISTINCT(host)) as requests_per_host"
    " FROM AccessLogs"
    " GROUP BY CAST(date_time as DATE)"
)
print "Q8. Average daily requests per host: "
avg_daily_requests_per_host.show()

# question 9: 40 different endpoints that generate response code 404
endpoints_404 = sqlContext.sql(
    "SELECT DISTINCT endpoint"
    " FROM AccessLogs"
    " WHERE response_code = 404"
    " LIMIT 40"
)
print "Q9. 40 different endpoints that generate response code 404: "
endpoints_404.show()

# question 10: 25 top endpoints that generate response code 404
endpoints_404_top = sqlContext.sql(
    "SELECT endpoint, COUNT(*) AS count"
    " FROM AccessLogs"
    " WHERE response_code = 404"
    " GROUP BY endpoint"
    " ORDER BY count DESC"
    " LIMIT 25"
)
print "Q10. Top 25 endpoints that generate code 404: "
endpoints_404_top.show()

# question 11: top 5 days that generate response code 404
endpoints_404_5days = sqlContext.sql(
    "SELECT CAST(date_time as DATE) AS day, COUNT(*) as count"
    " FROM AccessLogs"
    " WHERE response_code = 404"
    " GROUP BY CAST(date_time as DATE)"
    " ORDER BY count DESC"
    " LIMIT 5"
)
print "Q11. Top 5 days that generate code 404: "
endpoints_404_5days.show()
