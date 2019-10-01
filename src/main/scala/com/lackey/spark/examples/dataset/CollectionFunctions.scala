package com.lackey.spark.examples.dataset

import com.lackey.spark.examples.sqlfuns.FileInputHelper
import org.apache.calcite.avatica.ColumnMetaData
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.SparkStrategies
import org.apache.spark.sql.{Column, DataFrame, DataFrameReader, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


// See: https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-functions-collection.html
//
object CollectionFunctions extends App {

  val conf =
    new SparkConf().setAppName("blah").
      setMaster("local").set("spark.sql.shuffle.partitions", "2")
  val sparkSession = SparkSession.builder.config(conf).getOrCreate()
  val spark = sparkSession

  import spark.implicits._

  sparkSession.sparkContext.setLogLevel("ERROR")

  import FileInputHelper._

  setup




  import org.apache.spark.sql.types._
  val json =
    """{"name":"n2","id":"2","email":"n2@c1.com","company":{"cName":"c1","cId":1,"details":"d1"}}
      |{"name":"n1","email":"n1@c1.com","company":{"cName":"c1","cId":1,"details":"d1"}}
      |{"name":"n3","id":"3","email":"n3@c1.com","company":{"cName":"c1","cId":1,"details":"d1"}}
      |{"name":"n4","id":"4","email":"n4@c2.com","company":{"cName":"c2","cId":2,"details":"d2"}}
      |{"name":"n5","email":"n5@c2.com","company":{"cName":"c2","cId":2,"details":"d2"}}
      |{"name":"n6","id":"6","email":"n6@c2.com","company":{"cName":"c2","cId":2,"details":"d2"}}
      |{"name":"n7","id":"7","email":"n7@c3.com","company":{"cName":"c3","cId":3,"details":"d3"}}
      |{"name":"n8","id":"8","email":"n8@c3.com","company":{"cName":"c3","cId":3,"details":"d3"}}
      """.stripMargin('|')    // extra new line at end means we will read in a null JSON record. to be dropped at DROP.


  // Use the first JSON record to infer the schema rather than defining it manually
  // In particular we will be inferring that cId is an integer and we will treat it like that once we read in the
  // json with the inferred schema
  val jsonFile = "/tmp/input/json.json"
  createInputFile(json, jsonFile)
  val textDs = spark.read.textFile(jsonFile)
  val firstJsonRecord = textDs.first()
  val dfOfOneJsonString = List(firstJsonRecord).toDS()  // data frame of 1 record consisting of 1 string column
  val jsonDf = spark.read.json(dfOfOneJsonString)
  println("schema inferred from first record")
  jsonDf.printSchema
  jsonDf.show()
  val schemaFromFirstJsonRec = jsonDf.schema.json
  System.out.println("schemaFromFirstJsonRec (json formatted):" + schemaFromFirstJsonRec);
  val convertSchemaBackToStruct = DataType.fromJson(schemaFromFirstJsonRec).asInstanceOf[StructType]
  val fullJsonDf = spark.read.schema(convertSchemaBackToStruct).json(textDs)
  fullJsonDf.groupBy($"company.details".as("companyDetail")).agg(sum($"company.cId").as("cidSum")).show()







  // Now build the schema manually
  val schema2 = StructType(
    List(
      StructField("name", StringType),
      StructField("email", StringType),
      StructField("id", StringType),
      StructField("company",
        StructType(
          List(
            StructField("cName", StringType),
            StructField("cId", StringType),
            StructField("details", StringType)
          )
        )
      )
    )
  )

  val jsonStringsDf =
    List(json).toDF("lines").select(explode(split($"lines", "\\n")).as("line"))
  val parsedJsonDf =
    jsonStringsDf.select(from_json($"line", schema2).as("structo"))
  val expanded =
    parsedJsonDf.
      select($"structo.*", $"structo.company.*").
      drop("company").
      na.fill("0", Seq("id"))         // assign zero to id column for those records where w/ that column is null
  println("\n\nSchema of raw (string / not-yet-parsed-to-json) records, and dump of those raw records")
  textDs.printSchema()
  textDs.show(truncate = false)
  println("results with a null record due to the newline")
  expanded.show(false)              // This shows that we have one row with some nulls

  // drop the row w/ the null columns and display the result  (DROP: this is the null record resulting from empty line)
  val cleaned = expanded.na.drop().withColumn("id", $"id".cast(IntegerType))
  println("results with null removed via <dataframe>.na.drop()")
  cleaned .show()
  // RESULT:
  //+----+---------+---+-----+---+-------+
  //|name|    email| id|cName|cId|details|
  //+----+---------+---+-----+---+-------+
  //|  n1|n1@c1.com|  0|   c1|  1|     d1|
  //|  n2|n2@c1.com|  2|   c1|  1|     d1|
  //|  n3|n3@c1.com|  3|   c1|  1|     d1|
  //|  n4|n4@c2.com|  4|   c2|  2|     d2|
  //|  n5|n5@c2.com|  0|   c2|  2|     d2|
  //|  n6|n6@c2.com|  6|   c2|  2|     d2|
  //|  n7|n7@c3.com|  7|   c3|  3|     d3|
  //|  n8|n8@c3.com|  8|   c3|  3|     d3|
  //+----+---------+---+-----+---+-------+

  // Group by cName and show total of id column (as int) for each group
  val grouped = cleaned.groupBy($"cName").agg(sum($"id"))
  grouped.show()
  // RESULT:
  //+-----+--------------------+
  //|cName|sum(CAST(id AS INT))|
  //+-----+--------------------+
  //|   c1|                   5|
  //|   c3|                  15|
  //|   c2|                  10|
  //+-----+--------------------+



  // Show the dense rank of each user, where rank is determined by highest id
  import org.apache.spark.sql.expressions.Window
  val ranked = cleaned.withColumn("rank", dense_rank().over(Window.orderBy($"id".desc)))
  println("results with id treated as rank")
  ranked.show()
  //RESULT
  //+----+---------+---+-----+---+-------+----+
  //|name|    email| id|cName|cId|details|rank|
  //+----+---------+---+-----+---+-------+----+
  //|  n8|n8@c3.com|  8|   c3|  3|     d3|   1|
  //|  n7|n7@c3.com|  7|   c3|  3|     d3|   2|
  //|  n6|n6@c2.com|  6|   c2|  2|     d2|   3|
  //|  n4|n4@c2.com|  4|   c2|  2|     d2|   4|
  //|  n3|n3@c1.com|  3|   c1|  1|     d1|   5|
  //|  n2|n2@c1.com|  2|   c1|  1|     d1|   6|
  //|  n1|n1@c1.com|  0|   c1|  1|     d1|   7|
  //|  n5|n5@c2.com|  0|   c2|  2|     d2|   7|
  //+----+---------+---+-----+---+-------+----+


  val singersDF = Seq(
    ("beatles", "help|hey jude|help|help2"),
    ("romeo", "eres mia")
  ).toDF("name", "hit_songs")

  val delimParsedIntoArrayOfStringsDf = singersDF.withColumn(
    "hit_songs",
    split(col("hit_songs"), "\\|")
  )

  delimParsedIntoArrayOfStringsDf.show(truncate=false)
  delimParsedIntoArrayOfStringsDf.printSchema()

  val contains =
    delimParsedIntoArrayOfStringsDf.select(
      array_contains($"hit_songs", "help"),
      array_contains($"hit_songs", "not-in-list"))

  contains.show()   // should be true, false for first rec, false,false for next one after that

  Seq(Array(0,1,2)).toDF("array").withColumn("num", explode('array)).show
  // RESULT
  //+---------+---+
  //|    array|num|
  //+---------+---+
  //|[0, 1, 2]|  0|
  //|[0, 1, 2]|  1|
  //|[0, 1, 2]|  2|
  //+---------+---+


  // A UDF that takes an array of ints as an argument.

  val addOne : Seq[Int] => Seq[Int] =
    (ints:Seq[Int]) =>  ints.map(_ + 1)
  val addOneUdf = udf(addOne)

  val intArrayDf = Seq(Seq(4, 5, 6), Seq(9, 11)).toDF("integers")
  val ints: Seq[Int] =
    intArrayDf.select(addOneUdf($"integers").as("integersPlusOne")).
      first().getAs[Seq[Int]]("integersPlusOne")
  assert(ints.toList == List(5, 6, 7))

  

  // Some fun with from_json, schema and structs
  //
  val jstring = """{"strings": ["cat", "dog"]}"""
  val schemaFoo = StructType(
    List(
      StructField("strings", ArrayType(StringType))
    )
  )
  val df = Seq(jstring).toDF("strings")
  val stringArrayDf = df.select(from_json($"strings", schemaFoo).as("jsonStruct"))
  val singleStringDf = stringArrayDf.select(explode($"jsonStruct.strings").as("string")).orderBy("string")
  val retrieved = singleStringDf.first().getAs[String]("string")
  assert(retrieved  == "cat")




  // More with from_json
  //
  val jsons = Seq("""{ "id": 0 }""").toDF("json")

  import org.apache.spark.sql.types._

  val schema0 = new StructType().add($"id".int.copy(nullable = false))

  import org.apache.spark.sql.functions.from_json

  val jdf = jsons.select(from_json($"json", schema0) as "ids")
  println(""" schema for: { "id": 0 }""")
  jdf.printSchema()
  jdf.show
  // RESULT
  //+---+
  //|ids|
  //+---+
  //|[0]|
  //+---+

  import org.apache.spark.sql.types._

  val addressesSchema1 = new StructType()
    .add($"city".string)
    .add($"state".string)
    .add($"zip".string)
  val schema1 = new StructType()
    .add($"firstName".string)
    .add($"lastName".string)
    .add($"email".string)
    .add($"addresses".array(addressesSchema1))

  val schema3 = StructType(
    List(
      StructField("firstName", StringType),
      StructField("lastName", StringType),
      StructField("email", StringType),
      StructField("addresses",
        ArrayType(
          StructType(
            List(
              StructField("city", StringType),
              StructField("state", StringType),
              StructField("zip", StringType)
            )
          )
        )
      )
    )
  )


  // The above 2 schemas are built differently, but result in the same structure
  assert(schema1 == schema3)
  println("verfied schema constructed from json is identical to that constructed via StructType(...)")



  // Generate the JSON-encoded schema
  // That's one variant of the schema that from_json accepts... it can also accept the underlying 'raw'
  // schema structure (not converted to json)
  val schemaAsJson = schema1.json

  val rawJsons = Seq(
    """
    {
      "firstName" : "Jacek",
      "lastName" : "Laskowski",
      "email" : "jacek@japila.pl",
      "addresses" : [
        {
          "city" : "Warsaw",
          "state" : "N/A",
          "zip" : "02-791"
        },
        {
          "city" : "boston",
          "state" : "N/A",
          "zip" : "22"
        }
      ]
    }
  """).toDF("rawjson")
  val people = rawJsons
    .select(from_json($"rawjson", schema1, Map.empty[String, String]) as "json")
    .select("json.*") // <-- flatten the struct field
    .withColumn("address", explode($"addresses")) // <-- explode the array field
    .drop("addresses") // <-- no longer needed
    .select("firstName", "lastName", "email", "address.*") // <-- flatten the struct field
  people.show(false)
  // RESULT
  //+---------+---------+---------------+------+-----+------+
  //|firstName|lastName |email          |city  |state|zip   |
  //+---------+---------+---------------+------+-----+------+
  //|Jacek    |Laskowski|jacek@japila.pl|Warsaw|N/A  |02-791|
  //|Jacek    |Laskowski|jacek@japila.pl|boston|N/A  |22    |
  //+---------+---------+---------------+------+-----+------+



  // Array contains
  import org.apache.spark.sql.functions.array_contains

  val c: Column = array_contains(column = $"ids", value = 1)

  val ids = Seq(Seq(1, 2, 3), Seq(1), Seq(2, 3)).toDF("ids")
  val q = ids.filter(c)
  q.show
  // RESULT
  //+---------+
  //|      ids|
  //+---------+
  //|[1, 2, 3]|
  //|      [1]|
  //+---------+



  // to json function

  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._

  // Convenience function for turning JSON strings into DataFrames.
  def jsonToDataFrame(json: String, schema: StructType = null): DataFrame = {
    // SparkSessions are available with Spark 2.0+
    val reader: DataFrameReader = spark.read
    Option(schema).foreach(reader.schema)
    val jj: RDD[String] = spark.sparkContext.parallelize(Array(json))
    System.out.println("jj:" + jj.collect().toList);
    reader.json(jj)
  }


  val events: DataFrame = jsonToDataFrame("""
  {
    "a": {
      "b": 1
    }
  }
  """)

  events.show(false)


  events.select(to_json($"a").as("json")).show()
}




