
## Lab: Column Manipulation

**Step 1:** Using the Schema provided for us by the inferencer already provided, read in the file

**Step 2:** Print the Schema

**Step 3:** Rename `_c0` to something better like `id`

**Step 4:** Convert column `price` to Double Types

**Step 5:** Convert column `points` to Integer Types

**Step 4:** Convert columns `id` to Integer Type

**Step 5:** Print the Schema

**Step 6:** Show the Dataset

**Step 7:** Save the File with the name `winemag_column.csv` in the `../data` directory

**Step 8:** View the contents of the file


```scala
val winesDF = spark.read.format("csv")
                     .option("inferSchema", "true")
                     .option("header", "true")
                     .load("../data/winemag.csv")
winesDF.printSchema()
```


    Intitializing Scala interpreter ...



    Spark Web UI available at http://18febb47f317:4043
    SparkContext available as 'sc' (version = 2.4.3, master = local[*], app id = local-1564182374084)
    SparkSession available as 'spark'
    


    root
     |-- _c0: string (nullable = true)
     |-- country: string (nullable = true)
     |-- description: string (nullable = true)
     |-- designation: string (nullable = true)
     |-- points: string (nullable = true)
     |-- price: string (nullable = true)
     |-- province: string (nullable = true)
     |-- region_1: string (nullable = true)
     |-- region_2: string (nullable = true)
     |-- taster_name: string (nullable = true)
     |-- taster_twitter_handle: string (nullable = true)
     |-- title: string (nullable = true)
     |-- variety: string (nullable = true)
     |-- winery: string (nullable = true)
    
    




    winesDF: org.apache.spark.sql.DataFrame = [_c0: string, country: string ... 12 more fields]
    




```scala
import org.apache.spark.sql.types.{DoubleType, IntegerType}
val cleanWinesDF = winesDF
                     .withColumn("price", $"price".cast(DoubleType))
                     .withColumnRenamed("_c0", "id")
                     .withColumn("id", $"id".cast(IntegerType))
                     .withColumn("points", $"points".cast(IntegerType))
cleanWinesDF.printSchema()
```

    root
     |-- id: integer (nullable = true)
     |-- country: string (nullable = true)
     |-- description: string (nullable = true)
     |-- designation: string (nullable = true)
     |-- points: integer (nullable = true)
     |-- price: double (nullable = true)
     |-- province: string (nullable = true)
     |-- region_1: string (nullable = true)
     |-- region_2: string (nullable = true)
     |-- taster_name: string (nullable = true)
     |-- taster_twitter_handle: string (nullable = true)
     |-- title: string (nullable = true)
     |-- variety: string (nullable = true)
     |-- winery: string (nullable = true)
    
    




    import org.apache.spark.sql.types.{DoubleType, IntegerType}
    cleanWinesDF: org.apache.spark.sql.DataFrame = [id: int, country: string ... 12 more fields]
    




```scala
cleanWinesDF.show(10)
```

    +---+--------+--------------------+--------------------+------+-----+-----------------+-------------------+-----------------+------------------+---------------------+--------------------+------------------+-------------------+
    | id| country|         description|         designation|points|price|         province|           region_1|         region_2|       taster_name|taster_twitter_handle|               title|           variety|             winery|
    +---+--------+--------------------+--------------------+------+-----+-----------------+-------------------+-----------------+------------------+---------------------+--------------------+------------------+-------------------+
    |  0|   Italy|Aromas include tr...|        Vulkà Bianco|    87| null|Sicily & Sardinia|               Etna|             null|     Kerin O’Keefe|         @kerinokeefe|Nicosia 2013 Vulk...|       White Blend|            Nicosia|
    |  1|Portugal|This is ripe and ...|            Avidagos|    87| 15.0|            Douro|               null|             null|        Roger Voss|           @vossroger|Quinta dos Avidag...|    Portuguese Red|Quinta dos Avidagos|
    |  2|      US|Tart and snappy, ...|                null|    87| 14.0|           Oregon|  Willamette Valley|Willamette Valley|      Paul Gregutt|          @paulgwine |Rainstorm 2013 Pi...|        Pinot Gris|          Rainstorm|
    |  3|      US|Pineapple rind, l...|Reserve Late Harvest|    87| 13.0|         Michigan|Lake Michigan Shore|             null|Alexander Peartree|                 null|St. Julian 2013 R...|          Riesling|         St. Julian|
    |  4|      US|Much like the reg...|Vintner's Reserve...|    87| 65.0|           Oregon|  Willamette Valley|Willamette Valley|      Paul Gregutt|          @paulgwine |Sweet Cheeks 2012...|        Pinot Noir|       Sweet Cheeks|
    |  5|   Spain|Blackberry and ra...|        Ars In Vitro|    87| 15.0|   Northern Spain|            Navarra|             null| Michael Schachner|          @wineschach|Tandem 2011 Ars I...|Tempranillo-Merlot|             Tandem|
    |  6|   Italy|Here's a bright, ...|             Belsito|    87| 16.0|Sicily & Sardinia|           Vittoria|             null|     Kerin O’Keefe|         @kerinokeefe|Terre di Giurfo 2...|          Frappato|    Terre di Giurfo|
    |  7|  France|This dry and rest...|                null|    87| 24.0|           Alsace|             Alsace|             null|        Roger Voss|           @vossroger|Trimbach 2012 Gew...|    Gewürztraminer|           Trimbach|
    |  8| Germany|Savory dried thym...|               Shine|    87| 12.0|      Rheinhessen|               null|             null|Anna Lee C. Iijima|                 null|Heinz Eifel 2013 ...|    Gewürztraminer|        Heinz Eifel|
    |  9|  France|This has great de...|         Les Natures|    87| 27.0|           Alsace|             Alsace|             null|        Roger Voss|           @vossroger|Jean-Baptiste Ada...|        Pinot Gris| Jean-Baptiste Adam|
    +---+--------+--------------------+--------------------+------+-----+-----------------+-------------------+-----------------+------------------+---------------------+--------------------+------------------+-------------------+
    only showing top 10 rows
    
    
