
## Columns

* Columns simply are the columns of the `DataFrame`
* Columns are selectable and are easy to configuture
* Columns can be added and removed
* Columns represent a simple type like an integer or string, a complex type like an array or map, or a null value

## Bringing in the books dataset


```scala
val booksDF = spark.read.format("csv")
                     .option("inferSchema", "true")
                     .option("header", "true")
                     .load("../data/books.csv")

booksDF.printSchema()
```


    Intitializing Scala interpreter ...



    Spark Web UI available at http://18febb47f317:4042
    SparkContext available as 'sc' (version = 2.4.3, master = local[*], app id = local-1564178766270)
    SparkSession available as 'spark'
    


    root
     |-- bookID: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- authors: string (nullable = true)
     |-- average_rating: string (nullable = true)
     |-- isbn: string (nullable = true)
     |-- isbn13: string (nullable = true)
     |-- language_code: string (nullable = true)
     |-- # num_pages: string (nullable = true)
     |-- ratings_count: integer (nullable = true)
     |-- text_reviews_count: integer (nullable = true)
    
    




    booksDF: org.apache.spark.sql.DataFrame = [bookID: int, title: string ... 8 more fields]
    



## Representing `Column`

* Columns are represented by any of the following declarations
* The last two are possible by using `implicits` in Scala


```scala
import org.apache.spark.sql.functions.{col, column}

col("someColumnName")
column("someColumnName")
$"someColumnName"
'someColumnName
```




    import org.apache.spark.sql.functions.{col, column}
    res1: Symbol = 'someColumnName
    



## Extracting column representation from `DataFrame`

* We can also use `col` from the `DataFrame` to select one column


```scala
booksDF.col("average_rating")
```




    res2: org.apache.spark.sql.Column = average_rating
    



## Selecting columns to be displayed with `select`

* Select one or more columns using `select`
* You can use whatever column form you please


```scala
val subset = booksDF.select($"ratings_count", col("title"), 'authors)
subset.show()
```

    +-------------+--------------------+--------------------+
    |ratings_count|               title|             authors|
    +-------------+--------------------+--------------------+
    |      1944099|Harry Potter and ...|J.K. Rowling-Mary...|
    |      1996446|Harry Potter and ...|J.K. Rowling-Mary...|
    |      5629932|Harry Potter and ...|J.K. Rowling-Mary...|
    |         6267|Harry Potter and ...|        J.K. Rowling|
    |      2149872|Harry Potter and ...|J.K. Rowling-Mary...|
    |        38872|Harry Potter Boxe...|J.K. Rowling-Mary...|
    |           18|Unauthorized Harr...|W. Frederick Zimm...|
    |        27410|Harry Potter Coll...|        J.K. Rowling|
    |         3602|The Ultimate Hitc...|       Douglas Adams|
    |       240189|The Ultimate Hitc...|       Douglas Adams|
    |         4416|The Hitchhiker's ...|       Douglas Adams|
    |         1222|The Hitchhiker's ...|Douglas Adams-Ste...|
    |         2801|The Ultimate Hitc...|       Douglas Adams|
    |       228522|A Short History o...|Bill Bryson-Willi...|
    |         6993|Bill Bryson's Afr...|         Bill Bryson|
    |         2020|Bryson's Dictiona...|         Bill Bryson|
    |        68213|In a Sunburned Co...|         Bill Bryson|
    |        47490|I'm a Stranger He...|         Bill Bryson|
    |        43779|The Lost Continen...|         Bill Bryson|
    |        46397|Neither Here nor ...|         Bill Bryson|
    +-------------+--------------------+--------------------+
    only showing top 20 rows
    
    




    subset: org.apache.spark.sql.DataFrame = [ratings_count: int, title: string ... 1 more field]
    




```scala
val subset = booksDF.select($"ratings_count".as("num_ratings"), col("title"), 'authors)
subset.show()
```

    +-----------+--------------------+--------------------+
    |num_ratings|               title|             authors|
    +-----------+--------------------+--------------------+
    |    1944099|Harry Potter and ...|J.K. Rowling-Mary...|
    |    1996446|Harry Potter and ...|J.K. Rowling-Mary...|
    |    5629932|Harry Potter and ...|J.K. Rowling-Mary...|
    |       6267|Harry Potter and ...|        J.K. Rowling|
    |    2149872|Harry Potter and ...|J.K. Rowling-Mary...|
    |      38872|Harry Potter Boxe...|J.K. Rowling-Mary...|
    |         18|Unauthorized Harr...|W. Frederick Zimm...|
    |      27410|Harry Potter Coll...|        J.K. Rowling|
    |       3602|The Ultimate Hitc...|       Douglas Adams|
    |     240189|The Ultimate Hitc...|       Douglas Adams|
    |       4416|The Hitchhiker's ...|       Douglas Adams|
    |       1222|The Hitchhiker's ...|Douglas Adams-Ste...|
    |       2801|The Ultimate Hitc...|       Douglas Adams|
    |     228522|A Short History o...|Bill Bryson-Willi...|
    |       6993|Bill Bryson's Afr...|         Bill Bryson|
    |       2020|Bryson's Dictiona...|         Bill Bryson|
    |      68213|In a Sunburned Co...|         Bill Bryson|
    |      47490|I'm a Stranger He...|         Bill Bryson|
    |      43779|The Lost Continen...|         Bill Bryson|
    |      46397|Neither Here nor ...|         Bill Bryson|
    +-----------+--------------------+--------------------+
    only showing top 20 rows
    
    




    subset: org.apache.spark.sql.DataFrame = [num_ratings: int, title: string ... 1 more field]
    




```scala
subset.printSchema()
```

    root
     |-- num_ratings: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- authors: string (nullable = true)
    
    

## Programmatically accessing columns

* We can access the columns programmatically using `columns` from the `DataFrame`
* This returns an `Array[String]`


```scala
booksDF.columns
```




    res6: Array[String] = Array(bookID, title, authors, average_rating, isbn, isbn13, language_code, # num_pages, ratings_count, text_reviews_count)
    



## Renaming a column

* We can rename a column with `withColumnRenamed` with the first parameter being the old column, and the second being the new one.
* This returns a new `DataFrame`


```scala
val booksRenamedDF = booksDF.withColumnRenamed("text_reviews_count", "reviews_count")
booksRenamedDF.printSchema()
```

    root
     |-- bookID: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- authors: string (nullable = true)
     |-- average_rating: string (nullable = true)
     |-- isbn: string (nullable = true)
     |-- isbn13: string (nullable = true)
     |-- language_code: string (nullable = true)
     |-- # num_pages: string (nullable = true)
     |-- ratings_count: integer (nullable = true)
     |-- reviews_count: integer (nullable = true)
    
    




    booksRenamedDF: org.apache.spark.sql.DataFrame = [bookID: int, title: string ... 8 more fields]
    



## Creating a new column based on another

* Typically how we engineer our columns is to create another column based on a previous one using `withColumn`

In this example, we are bringing in the `booksDF` and we notice the `# num_pages` is using a `StringType`, so let's use a `cast` and create a new column with a better column name `num_pages` and ensure it is an `IntegerType`


```scala
import org.apache.spark.sql.types.IntegerType
val convertedDF = booksRenamedDF.withColumn("num_pages", col("# num_pages").cast(IntegerType))
convertedDF.printSchema()
```

    root
     |-- bookID: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- authors: string (nullable = true)
     |-- average_rating: string (nullable = true)
     |-- isbn: string (nullable = true)
     |-- isbn13: string (nullable = true)
     |-- language_code: string (nullable = true)
     |-- # num_pages: string (nullable = true)
     |-- ratings_count: integer (nullable = true)
     |-- reviews_count: integer (nullable = true)
     |-- num_pages: integer (nullable = true)
    
    




    import org.apache.spark.sql.types.IntegerType
    convertedDF: org.apache.spark.sql.DataFrame = [bookID: int, title: string ... 9 more fields]
    



## Dropping the columns

* We can drop the columns using `drop`
* Since we have a table to with two columns that represent pages, we can drop the one we don't want


```scala
val finalDF = convertedDF.drop("# num_pages")
finalDF.printSchema()
```

    root
     |-- bookID: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- authors: string (nullable = true)
     |-- average_rating: string (nullable = true)
     |-- isbn: string (nullable = true)
     |-- isbn13: string (nullable = true)
     |-- language_code: string (nullable = true)
     |-- ratings_count: integer (nullable = true)
     |-- reviews_count: integer (nullable = true)
     |-- num_pages: integer (nullable = true)
    
    




    finalDF: org.apache.spark.sql.DataFrame = [bookID: int, title: string ... 8 more fields]
    




```scala
finalDF.show(10)
```

    +------+--------------------+--------------------+--------------+----------+-------------+-------------+-------------+-------------+---------+
    |bookID|               title|             authors|average_rating|      isbn|       isbn13|language_code|ratings_count|reviews_count|num_pages|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+-------------+-------------+---------+
    |     1|Harry Potter and ...|J.K. Rowling-Mary...|          4.56|0439785960|9780439785969|          eng|      1944099|        26249|      652|
    |     2|Harry Potter and ...|J.K. Rowling-Mary...|          4.49|0439358078|9780439358071|          eng|      1996446|        27613|      870|
    |     3|Harry Potter and ...|J.K. Rowling-Mary...|          4.47|0439554934|9780439554930|          eng|      5629932|        70390|      320|
    |     4|Harry Potter and ...|        J.K. Rowling|          4.41|0439554896|9780439554893|          eng|         6267|          272|      352|
    |     5|Harry Potter and ...|J.K. Rowling-Mary...|          4.55|043965548X|9780439655484|          eng|      2149872|        33964|      435|
    |     8|Harry Potter Boxe...|J.K. Rowling-Mary...|          4.78|0439682584|9780439682589|          eng|        38872|          154|     2690|
    |     9|Unauthorized Harr...|W. Frederick Zimm...|          3.69|0976540606|9780976540601|        en-US|           18|            1|      152|
    |    10|Harry Potter Coll...|        J.K. Rowling|          4.73|0439827604|9780439827607|          eng|        27410|          820|     3342|
    |    12|The Ultimate Hitc...|       Douglas Adams|          4.38|0517226952|9780517226957|          eng|         3602|          258|      815|
    |    13|The Ultimate Hitc...|       Douglas Adams|          4.38|0345453743|9780345453747|          eng|       240189|         3954|      815|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+-------------+-------------+---------+
    only showing top 10 rows
    
    

## Bring it all together


```scala
val final2 = booksDF.withColumnRenamed("text_reviews_count", "reviews_count")
                    .withColumn("num_pages", col("# num_pages").cast(IntegerType))
                    .drop("# num_pages")
final2.printSchema()
```

    root
     |-- bookID: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- authors: string (nullable = true)
     |-- average_rating: string (nullable = true)
     |-- isbn: string (nullable = true)
     |-- isbn13: string (nullable = true)
     |-- language_code: string (nullable = true)
     |-- ratings_count: integer (nullable = true)
     |-- reviews_count: integer (nullable = true)
     |-- num_pages: integer (nullable = true)
    
    




    final2: org.apache.spark.sql.DataFrame = [bookID: int, title: string ... 8 more fields]
    




```scala
final2.show(10)
```

    +------+--------------------+--------------------+--------------+----------+-------------+-------------+-------------+-------------+---------+
    |bookID|               title|             authors|average_rating|      isbn|       isbn13|language_code|ratings_count|reviews_count|num_pages|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+-------------+-------------+---------+
    |     1|Harry Potter and ...|J.K. Rowling-Mary...|          4.56|0439785960|9780439785969|          eng|      1944099|        26249|      652|
    |     2|Harry Potter and ...|J.K. Rowling-Mary...|          4.49|0439358078|9780439358071|          eng|      1996446|        27613|      870|
    |     3|Harry Potter and ...|J.K. Rowling-Mary...|          4.47|0439554934|9780439554930|          eng|      5629932|        70390|      320|
    |     4|Harry Potter and ...|        J.K. Rowling|          4.41|0439554896|9780439554893|          eng|         6267|          272|      352|
    |     5|Harry Potter and ...|J.K. Rowling-Mary...|          4.55|043965548X|9780439655484|          eng|      2149872|        33964|      435|
    |     8|Harry Potter Boxe...|J.K. Rowling-Mary...|          4.78|0439682584|9780439682589|          eng|        38872|          154|     2690|
    |     9|Unauthorized Harr...|W. Frederick Zimm...|          3.69|0976540606|9780976540601|        en-US|           18|            1|      152|
    |    10|Harry Potter Coll...|        J.K. Rowling|          4.73|0439827604|9780439827607|          eng|        27410|          820|     3342|
    |    12|The Ultimate Hitc...|       Douglas Adams|          4.38|0517226952|9780517226957|          eng|         3602|          258|      815|
    |    13|The Ultimate Hitc...|       Douglas Adams|          4.38|0345453743|9780345453747|          eng|       240189|         3954|      815|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+-------------+-------------+---------+
    only showing top 10 rows
    
    

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
