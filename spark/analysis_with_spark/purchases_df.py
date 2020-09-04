import os
import sys


from pyspark.sql import functions as sf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

spark = SparkSession \
    .builder \
    .appName("individual_assignment_df") \
    .getOrCreate()

# DataFrame creation from CSV file
df_stock = spark.read.option("header", "true") \
               .option("inferSchema", "true") \
               .csv("data/stock.csv")

df_purchases = spark.read.option("inferSchema", "true") \
                .json("data/purchases.json")

# Checking that the schema is in place and we can access lon and lat
df_purchases.printSchema()
df_purchases.select("location.lon").show()

# question 1: Top 10 most purchased products
top_10_purchased = df_purchases.groupby(df_purchases.product_id).count().sort('count', ascending=False)
print "Q1. Top 10 most purchased products: "
top_10_purchased.show(10)

# question 2: Purchase percentage of each product type (item_type)
purchase_perc_by_type = df_purchases.groupby(df_purchases.item_type).agg((sf.count('product_id')/df_purchases.count()).alias('percentage_sold'))
print "Q2. Purchase percentage of each product type : "
purchase_perc_by_type.show()

# question 3: Shop that has sold more products
best_shop = df_purchases.groupby(df_purchases.shop_id).count().sort('count', ascending=False)
print "Q3. Shop that has sold more products : "
best_shop.show(1)

# question 4: Shop that has billed more money
most_billed_shop = df_purchases.groupby(df_purchases.shop_id).agg((sf.sum(df_purchases.price)).alias('revenue')).sort('revenue', ascending=False)
print "Q4. Shop that has billed more money : "
most_billed_shop.show(1)

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
print "Q5. geographic areas : "
df_purchases_geog.show()

# question 5a: In which area is PayPal most used
most_area_paypal = df_purchases_geog.where(df_purchases_geog.payment_type == "paypal").groupby(df_purchases_geog.geographic_area).count().sort('count', ascending=False)
print "Q5a. PayPal is most used in those geographic areas : "
most_area_paypal.show(1)

# question 5b: Top 3 most purchased products in each area
from pyspark.sql import Window
from pyspark.sql.functions import rank, count, col

df_count = df_purchases_geog.groupby(df_purchases_geog.geographic_area, df_purchases_geog.product_id ).agg(sf.count(df_purchases_geog.product_id).alias('count_product'))
window = Window.partitionBy('geographic_area').orderBy(col('count_product').desc())

most_purchased_products_area = df_count.select("geographic_area", "product_id",rank().over(window).alias('rank'))\
                                                  .where(col('rank') <= 3).sort('geographic_area', ascending = True)

print "Q5b. Top 3 most purchased products in each area : "
most_purchased_products_area.show()


# question 5c: Area that has billed less money
area_less_billed = df_purchases_geog.groupby(df_purchases_geog.geographic_area).agg((sf.sum(df_purchases.price)).alias('revenue')).sort('revenue', ascending=True)
print "Q5c. Area that has billed less money : "
area_less_billed.show(1)

# question 6: Products that do not have enough stock for purchases made

purchases = df_purchases.select("product_id").groupby("product_id").agg(sf.count(df_purchases.product_id).alias('bought'))
stock_products_purchases = df_stock.join(purchases, "product_id")
not_enough = stock_products_purchases.where(stock_products_purchases.bought > stock_products_purchases.quantity)

print "Q6. Products that do not have enough stock for purchases made: "
not_enough.show()