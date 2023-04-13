val df = spark.read.option("header",true).csv("sales.csv")
df.printSchema()
/*
|-- BOROUGH: string (nullable = true)
 |-- NEIGHBORHOOD: string (nullable = true)
 |-- BUILDING CLASS CATEGORY: string (nullable = true)
 |-- TAX CLASS AS OF FINAL ROLL: string (nullable = true)
 |-- BLOCK: string (nullable = true)
 |-- LOT: string (nullable = true)
 |-- EASE-MENT: string (nullable = true)
 |-- BUILDING CLASS AS OF FINAL ROLL: string (nullable = true)
 |-- ADDRESS: string (nullable = true)
 |-- APARTMENT NUMBER: string (nullable = true)
 |-- ZIP CODE: string (nullable = true)
 |-- RESIDENTIAL UNITS: string (nullable = true)
 |-- COMMERCIAL UNITS: string (nullable = true)
 |-- TOTAL UNITS: string (nullable = true)
 |-- LAND SQUARE FEET: string (nullable = true)
 |-- GROSS SQUARE FEET: string (nullable = true)
 |-- YEAR BUILT: string (nullable = true)
 |-- TAX CLASS AT TIME OF SALE: string (nullable = true)
 |-- BUILDING CLASS AT TIME OF SALE: string (nullable = true)
 |-- SALE PRICE: string (nullable = true)
 |-- SALE DATE: string (nullable = true)
 |-- Latitude: string (nullable = true)
 |-- Longitude: string (nullable = true)
 |-- Community Board: string (nullable = true)
 |-- Council District: string (nullable = true)
 |-- Census Tract: string (nullable = true)
 |-- BIN: string (nullable = true)
 |-- BBL: string (nullable = true)
 |-- NTA: string (nullable = true)
 */

//Unwanted Columns
 val dfPostDrop = df.drop("TAX CLASS AS OF FINAL ROLL","BLOCK", 
 "LOT","EASE-MENT","BUILDING CLASS AS OF FINAL ROLL","Latitude","Longitude",
 "Community Board","Census Tract","BBL","NTA","Council District","GROSS SQUARE FEET")
/*
 |-- BOROUGH: string (nullable = true)
 |-- NEIGHBORHOOD: string (nullable = true)
 |-- BUILDING CLASS CATEGORY: string (nullable = true)
 |-- ADDRESS: string (nullable = true)
 |-- APARTMENT NUMBER: string (nullable = true)
 |-- ZIP CODE: string (nullable = true)
 |-- RESIDENTIAL UNITS: string (nullable = true)
 |-- COMMERCIAL UNITS: string (nullable = true)
 |-- TOTAL UNITS: string (nullable = true)
 |-- LAND SQUARE FEET: string (nullable = true)
 |-- YEAR BUILT: string (nullable = true)
 |-- TAX CLASS AT TIME OF SALE: string (nullable = true)
 |-- BUILDING CLASS AT TIME OF SALE: string (nullable = true)
 |-- SALE PRICE: string (nullable = true)
 |-- SALE DATE: string (nullable = true)
 |-- BIN: string (nullable = true)
 */
dfPostDrop.registerTempTable("sales")

//Typecasting columns
val salesTyped = spark.sql("SELECT BOROUGH as borough, NEIGHBORHOOD as neighborhood, `BUILDING CLASS CATEGORY` as building_class, ADDRESS as address, `APARTMENT NUMBER` as apt_number, CAST(`ZIP CODE` as INT) as zipcode, CAST(`RESIDENTIAL UNITS` as INT) as res_units, CAST(`COMMERCIAL UNITS` as INT)com_units, CAST(`TOTAL UNITS` as INT) as total_units, CAST(`LAND SQUARE FEET` as INT) as land_sq_ft, CAST(`YEAR BUILT` as INT) as year_built,CAST(`TAX CLASS AT TIME OF SALE` as INT) as tax_class_toi,`BUILDING CLASS AT TIME OF SALE` as building_class_toi, CAST(`SALE PRICE` as INT) as sale_price, to_Date(`SALE DATE`, 'MM/dd/yyyy') as date, CAST(`BIN` as INT) as BIN FROM sales")
/*
val salesTyped = spark.sql("SELECT BOROUGH as borough, 
                                   NEIGHBORHOOD as neighborhood, 
                                   `BUILDING CLASS CATEGORY` as building_class, 
                                   ADDRESS as address, 
                                   `APARTMENT NUMBER` as apt_number, 
                                   CAST(`ZIP CODE` as INT) as zipcode, 
                                   CAST(`RESIDENTIAL UNITS` as INT) as res_units, 
                                   CAST(`COMMERCIAL UNITS` as INT)com_units, 
                                   CAST(`TOTAL UNITS` as INT) as total_units, 
                                   CAST(`LAND SQUARE FEET` as INT) as land_sq_ft, 
                                   CAST(`YEAR BUILT` as INT) as year_built,
                                   CAST(`TAX CLASS AT TIME OF SALE` as INT) as tax_class_tos,
                                   `BUILDING CLASS AT TIME OF SALE` as building_class_toi,
                                   CAST(`SALE PRICE` as INT) as sale_price, 
                                   to_Date(`SALE DATE`, 'MM/dd/yyyy') as date,
                                   CAST(`BIN` as INT) as BIN
                            FROM sales")
result:
 |-- borough: string (nullable = true)
 |-- neighborhood: string (nullable = true)
 |-- building_class: string (nullable = true)
 |-- address: string (nullable = true)
 |-- apt_number: string (nullable = true)
 |-- zipcode: integer (nullable = true)
 |-- res_units: integer (nullable = true)
 |-- com_units: integer (nullable = true)
 |-- total_units: integer (nullable = true)
 |-- land_sq_ft: integer (nullable = true)
 |-- year_built: integer (nullable = true)
 |-- tax_class_toi: integer (nullable = true)
 |-- building_class_toi: string (nullable = true)
 |-- sale_price: integer (nullable = true)
 |-- date: date (nullable = true)
 |-- BIN: integer (nullable = true)
*/
salesTyped.registerTempTable("salesT")

/*
---------------------
Initial Code Analysis
--------------------- 
* Find distinct values in columns
* Mean, median, and mode of numerical data or counts of text data
* Optional: standard deviation of values
*/


//Borough
spark.sql("SELECT  borough, count(borough) FROM salesT GROUP BY borough").show()
/*
+-------------+--------------+
|      borough|count(borough)|
+-------------+--------------+
|            3|         96958|
|            5|         35174|
|       QUEENS|         51513|
|     BROOKLYN|         48864|
|        BRONX|         14337|
|            1|         74301|
|            4|        107707|
|    MANHATTAN|         35636|
|STATEN ISLAND|         17424|
|            2|         30919|
+-------------+--------------+
Will need to standardize...
---> use zipcode to map to boroughs
        from https://bklyndesigns.com/new-york-city-zip-code/#:~:text=In%20New%20York%20City%2C%20the%20zip%20codes%20are,10301-10314%20Bronx%3A%2010451-10475%20Queens%3A%2011004-11109%2C%2011351-11697%20Brooklyn%3A%2011201-11256
        Manhattan: 10001-10282
        Staten Island: 10301-10314
        Bronx: 10451-10475
        Queens: 11004-11109, 11351-11697
        Brooklyn: 11201-11256
*/
val fixZip = spark.sql("SELECT CASE WHEN zipcode <=10282 AND zipcode >=10001 THEN 'MANHATTAN' WHEN zipcode <=10314 AND zipcode >=10301 THEN 'STATEN ISLAND' WHEN zipcode <=10475 AND zipcode >=10451 THEN 'BRONX' WHEN zipcode <=11256 AND zipcode >=11201 THEN 'BROOKLYN' WHEN ((zipcode <=11109 AND zipcode >=11004) OR (zipcode <=11697 AND zipcode >=11351)) THEN 'QUEENS' ELSE 'MISMATCH' END AS borough_mapped,* FROM salesT")
/*
val fixZip = spark.sql("SELECT 
                            CASE 
                                WHEN zipcode <=10282 AND zipcode >=10001 THEN 'MANHATTAN' 
                                WHEN zipcode <=10314 AND zipcode >=10301 THEN 'STATEN ISLAND' 
                                WHEN zipcode <=10475 AND zipcode >=10451 THEN 'BRONX' 
                                WHEN zipcode <=11256 AND zipcode >=11201 THEN 'BROOKLYN' 
                                WHEN ((zipcode <=11109 AND zipcode >=11004) OR (zipcode <=11697 AND zipcode >=11351)) THEN 'QUEENS' 
                                ELSE 'MISMATCH' 
                            END AS borough_mapped,* 
                        FROM salesT")
*/
val fixedZipCode = fixZip.filter("zipcode >=10001").filter("zipcode != null")

fixZip.filter("zipcode >= 10001").na.drop().registerTempTable("fixZipT")

spark.sql("select count(*),borough_mapped,borough from fixZipT where zipcode >= 10001 and zipcode is not null group by borough_mapped,borough").show()
/*
+--------+--------------+-------------+                                         
|count(1)|borough_mapped|      borough|
+--------+--------------+-------------+
|     671| STATEN ISLAND|            5|
|     565| STATEN ISLAND|STATEN ISLAND|
|    3937|      BROOKLYN|     BROOKLYN|
|       2|         BRONX|            1|
|    8253|      BROOKLYN|            3|
|     958|         BRONX|            2|
|   11424|     MANHATTAN|            1|
|    4275|        QUEENS|            4|
|    2320|        QUEENS|       QUEENS|
|     286|         BRONX|        BRONX|
|    4943|     MANHATTAN|    MANHATTAN|
+--------+--------------+-------------+
spark.sql("select count(*),borough_mapped,borough from fixZipT
         where zipcode >= 10001 and zipcode is not null group by borough_mapped,borough").show()
*/

val res30 = fixZip.withColumn("borough",when(col("borough").equalTo("1"),"MANHATTAN").otherwise(col("borough")));
val res31 = res30.withColumn("borough",when(col("borough").equalTo("2"),"BRONX").otherwise(col("borough")));
val res32 = res31.withColumn("borough",when(col("borough").equalTo("3"),"BROOKLYN").otherwise(col("borough")));
val res33 = res32.withColumn("borough",when(col("borough").equalTo("4"),"QUEENS").otherwise(col("borough")));
val res34 = res33.withColumn("borough",when(col("borough").equalTo("5"),"STATEN ISLAND").otherwise(col("borough")));
res34.registerTempTable("boroFixed")
val df = spark.sql("select * from boroFixed where zipcode >= 10001 and zipcode is not null").drop("borough_mapped")

//Want only residential units
//--> subset to tax classes 1 & 2
val sales = df.filter("tax_class_toi <=2")

//Want only cash considerating purchases (avoid inheritance and corporate acquisitions)
val salesdf= sales.filter("sale_price >0") 

salesdf.agg(min($"sale_price"),max($"sale_price")).show()
/*
+---------------+---------------+------------------+
|min(sale_price)|max(sale_price)|   avg(sale_price)|
+---------------+---------------+------------------+
|              1|     1932900000|1752224.9680748163|
+---------------+---------------+------------------+
*/

//Land_sq_units..seems the most reasonable minimum
val df3 = salesdf.filter("land_sq_ft >= 10")
df3.agg(min($"land_sq_ft"),max($"land_sq_ft"),avg($"land_sq_ft")).show()
/*
+---------------+---------------+-----------------+
|min(land_sq_ft)|max(land_sq_ft)|  avg(land_sq_ft)|
+---------------+---------------+-----------------+
|             10|        7649136|8972.540844258752|
+---------------+---------------+-----------------+
*/

salesdf.write.csv("hdfs://nyu-dataproc-m/user/jue203_nyu_edu/dirToShareAccess/salesFinal.csv")

//Joining datasets...trying to join by BIN for fun
val complaints = spark.read.csv("dirToShareAccess/complaints.csv")
//Aggregating complaints dataset...
complaints.registerTempTable("complaintsT")
val comptoJoin = spark.sql("SELECT `_c3` as BIN,COUNT(`_c0`) as num_complaints,SUM(CASE WHEN `_c6` LIKE '1' THEN 1 ELSE 0 END) as num_illegal, SUM(CASE WHEN `_c6` LIKE '0' THEN 1 ELSE 0 END) as num_legal  FROM complaintsT group by BIN")
/*
+-------+--------------+-----------+---------+
|    BIN|num_complaints|num_illegal|num_legal|
+-------+--------------+-----------+---------+
|2007940|             9|          1|        8|
|2102859|             4|          1|        3|
|2042268|            30|         14|       16|
|2813026|             3|          3|        0|
|2010800|            11|          0|       11|
|2124504|             3|          0|        3|
|2004268|            26|          3|       23|
|2113158|            22|          2|       20|
|2008079|            33|         12|       21|
|2000229|             2|          1|        1|
|2006556|            94|          3|       91|
|2058925|             4|          3|        1|
|2016389|            10|          1|        9|
|2010442|             4|          1|        3|
|2072948|             4|          2|        2|
|2042778|            12|          9|        3|
|2001020|             7|          1|        6|
|2003013|            16|          4|       12|
|2092993|             1|          0|        1|
|2072731|            19|          7|       12|
+-------+--------------+-----------+---------+
*/
salesdf.registerTempTable("salesT")
comptoJoin.registerTempTable("complaintsT")
spark.sql("SELECT s.borough,s.sale_price,c.num_complaints,c.num_illegal,c.num_legal FROM salesT s INNER JOIN complaintsT c where s.BIN == c.BIN").show()
/*
+--------+----------+--------------+-----------+---------+                      
| borough|sale_price|num_complaints|num_illegal|num_legal|
+--------+----------+--------------+-----------+---------+
|BROOKLYN|    382530|             3|          0|        3|
|BROOKLYN|    572500|            16|          5|       11|
|BROOKLYN|    440000|            16|          5|       11|
|BROOKLYN|    588000|            16|          5|       11|
|BROOKLYN|    555000|            16|          5|       11|
|BROOKLYN|    530000|            16|          5|       11|
|BROOKLYN|    598000|            16|          5|       11|
|BROOKLYN|    588000|            16|          5|       11|
|BROOKLYN|   1350000|             3|          1|        2|
|BROOKLYN|    490000|            11|          0|       11|
|BROOKLYN|    839000|            11|          0|       11|
|BROOKLYN|    715000|            11|          0|       11|
|BROOKLYN|    667660|            11|          0|       11|
|BROOKLYN|    647000|            11|          0|       11|
|BROOKLYN|    485000|            11|          0|       11|
|BROOKLYN|    581900|            11|          0|       11|
|BROOKLYN|   1025000|            11|          0|       11|
|BROOKLYN|    645000|            11|          0|       11|
|BROOKLYN|    575000|            11|          0|       11|
|BROOKLYN|   1925000|            11|          0|       11|
+--------+----------+--------------+-----------+---------+
*/

spark.sql("SELECT COUNT(*) FROM (SELECT s.borough,s.sale_price,c.num_complaints,c.num_illegal,c.num_legal FROM sales s INNER JOIN complaintsJoin c where s.BIN == c.BIN) as joined").show()
/*
+--------+                                                                      
|count(1)|
+--------+
|  246312|
+--------+

Plenty to work with still!
*/
val joineddf = spark.sql("SELECT s.*,c.num_complaints,c.num_illegal,c.num_legal FROM salesT s INNER JOIN complaintsT c where s.BIN == c.BIN")
joineddf.write.csv("hdfs://nyu-dataproc-m/user/jue203_nyu_edu/dirToShareAccess/joineddf")
joineddf.printSchema()
/*
 |-- borough: string (nullable = true)
 |-- neighborhood: string (nullable = true)
 |-- building_class: string (nullable = true)
 |-- address: string (nullable = true)
 |-- apt_number: string (nullable = true)
 |-- zipcode: integer (nullable = true)
 |-- res_units: integer (nullable = true)
 |-- com_units: integer (nullable = true)
 |-- total_units: integer (nullable = true)
 |-- land_sq_ft: integer (nullable = true)
 |-- year_built: integer (nullable = true)
 |-- tax_class_toi: integer (nullable = true)
 |-- building_class_toi: string (nullable = true)
 |-- sale_price: integer (nullable = true)
 |-- date: date (nullable = true)
 |-- BIN: integer (nullable = true)
 |-- num_complaints: long (nullable = false)
 |-- num_illegal: long (nullable = true)
 |-- num_legal: long (nullable = true)
*/

joineddf.count
//res33: Long = 246312    

//Analysis