import os
import sys

from pyspark.sql import functions as sf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

spark = SparkSession \
    .builder \
    .appName("individual_assignment_sql") \
    .getOrCreate()

# DataFrame creation from CSV file
df_stock = spark.read.option("header", "true") \
               .option("inferSchema", "true") \
               .csv("data/stock.csv")

df_purchases = spark.read.option("inferSchema", "true") \
                .json("data/purchases.json")

df_purchases.registerTempTable("Purchases")
df_stock.registerTempTable("Stock")

# question 1: Top 10 most purchased products

top_10_purchased = spark.sql('Select product_id, count(product_id) from Purchases \
                             Group by product_id \
                             Order by count(product_id) desc \
                             Limit 10')
print "Q1. Top 10 most purchased products: "
top_10_purchased.show()

# question 2: Purchase percentage of each product type (item_type)
purchase_perc_by_type = spark.sql('Select item_type, (count(*)/sum(count(*)) over()) as percentage from Purchases \
                                Group by item_type')
print "Q2. Purchase percentage of each product type : "
purchase_perc_by_type.show()

# question 3: Shop that has sold more products
best_shop = spark.sql('Select shop_id, count(product_id) from Purchases \
                       Group by shop_id \
                       Order by count(product_id) desc \
                       Limit 1')
print "Q3. Shop that has sold more products : "
best_shop.show()

# question 4: Shop that has billed more money
most_billed_shop = spark.sql('Select shop_id, sum(price) as revenue from Purchases \
                       Group by shop_id \
                       Order by revenue desc \
                       Limit 1')
print "Q4. Shop that has sold billed more money : "
most_billed_shop.show()

# question 5: Divide world into 5 geographical areas based in longitude (location.lon) and add a column with geographical area name

def buckets(x):
    if x <= -108:
        return 1
    elif x <= -36:
        return 2
    elif x <= 36:
        return 3
    elif x <= 108:
        return 4
    elif x <= 180:
        return 5

geographic_area = udf(buckets)
df_purchases_geog = df_purchases.withColumn("geographic_area", geographic_area("location.lon"))
df_purchases_geog.registerTempTable("Geog_purchases")

# question 5a: In which area is PayPal most used
most_area_paypal = spark.sql('Select geographic_area, count(*) as count from Geog_purchases \
                             Where payment_type = "paypal" \
                             Group by geographic_area \
                             Order by count desc \
                             Limit 1')
print "Q5a. PayPal is most used in those geographic areas : "
most_area_paypal.show(1)

# question 5b: Top 3 most purchased products in each area  (window function idea from https://dzone.com/articles/how-to-write-efficient-top-n-queries-in-sql)
most_purchased_items_area = spark.sql('Select * From ( \
                                    Select \
                                        geographic_area, product_id, count(product_id) as count, \
                                        rank() OVER (Partition BY geographic_area ORDER BY count(product_id) DESC) rank \
                                        From Geog_purchases \
                                    Group by  geographic_area, product_id\
                                                                        ) t \
                                    Where rank <= 3 \
                                    Order by geographic_area, count desc')

# This thing will return more than 3 records if the rank was equal for several products, if we wanted strictly 3 rows we could use row_number() instead of rank
# But I think that rank here makes more sense

print "Q5b. Top 3 most purchased products in each area: "
most_purchased_items_area.show()

# question 5c: Area that has billed less money
area_less_billed = spark.sql('Select geographic_area, sum(price) as revenue from Geog_purchases \
                             Group by geographic_area \
                             Order by revenue asc \
                             Limit 1')
print "Q5c. Area that has billed less money : "
area_less_billed.show()

# question 6: Products that do not have enough stock for purchases made

not_enough = spark.sql('Select Stock.product_id, quantity, bought \
                    From Stock join (Select product_id, count(product_id) as bought from Geog_purchases group by product_id) as grouped \
                    Where Stock.product_id = grouped.product_id and grouped.bought>quantity')


print "Q6. Products that do not have enough stock for purchases made: "
not_enough.show()