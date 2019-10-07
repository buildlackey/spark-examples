package com.lackey.spark.examples.readwrite

import java.io.File

import org.apache.spark.sql.{Dataset, Row, SparkSession}


// This test creates a large-ish number of rows so that we are sure that our items hash into all the buckets available.
// As expected from the test name we verify that the number of parquet files created by the script ==  num buckets X num partitions
// Note that if we only created a handful of records we found we did not get the expected number of files written out.
// I suspect that is because in the smallish set some of the records might have hashed to the same bucket.
//
object VerifyNumberOfFilesCreatedIsNumBucketsTimesNumPartitions extends App {
  val sparkSession =
    SparkSession.builder().
      appName("simple").
      master("local[3]").getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")
  val spark = sparkSession

  import spark.implicits._

  import scala.sys.process._
  "rm -rf spark-warehouse/ratboy".!

  spark.sql("drop table if exists ratboy").show()

  case class Person(first: String, last: String, age: Integer)

  val df = ( 1 to 1000).map(i => Person(s"moe_$i", "last", i)).toDF()

  val numBuckets = 5
  val numPartitions = df.rdd.getNumPartitions
  val numParquetFilesExpected =  numBuckets *  numPartitions
  df.write.option("mode", "overwrite").bucketBy(numBuckets, "first").saveAsTable("ratboy")

  assert(
    new File("spark-warehouse/ratboy").
      listFiles().count(f => f.getName.endsWith("parquet")) == numParquetFilesExpected )
}


object PartitionsBucketsDDLFormat extends App {
  val sparkSession =
    SparkSession.builder().
      appName("simple").
      enableHiveSupport().
      master("local[3]").getOrCreate()


  sparkSession.sparkContext.setLogLevel("ERROR")
  val spark = sparkSession

  import spark.implicits._

  // Partitioning demo
  //
  case class Person(first: String, last: String, age: Integer)

  val df = List(
    Person("moe", "x", 2),
    Person("mob", "x", 2),
    Person("mob", "x", 2),
    Person("zike", "y", 2),
    Person("zike", "y", 3),
    Person("red", "y", 2),
    Person("red", "z", 2)).toDF()


  // Overwrite existing and verify partition directories are as expected
  // (for the four names: moe,mob,zike,red)
  //
  df.write.partitionBy("first").mode("overwrite").csv("/tmp/output.mouse")
  assert(new File("/tmp/output.mouse").listFiles().count(f => f.getName.startsWith("first=")) == 4)


  // Bucketing test - this doesn't verify the actual number of buckets created. See the above test for that !
  //
  spark.sql("drop table if exists fogbox").show()
  println("show tables / current working dir: " + System.getProperty("user.dir"))
  spark.sql("show tables").show()     // this shows the table is gone, but... seems to linger when run from IDE

  // So,  we need to delete the fogbox directory. If we don't we can't recreate the table. This doesn't happen in
  // spark shell.. not sure why ...  punting for now
  import scala.sys.process._
  "rm -rf spark-warehouse/fogbox".!

  import org.apache.spark.sql.Row
  case class Parson(first: String, last: String, age: Integer)
  val pdf = List(
    Parson("moe", "x", 4),
    Parson("jox", "x", 3),
    Parson("pop", "x", 1),
    Parson("bob", "y", 2)).toDF()

  // BAD:
  // for me, this gives: AnalysisException: 'save' does not support bucketBy right now
  // pdf.write.option("mode","overwrite").bucketBy(3, "last", "age").parquet("/tmp/rrfogbox")

  println(s"number of partitions of rdd will affect # of buckets written: ${pdf.rdd.getNumPartitions}")


  pdf.write.option("mode", "overwrite").bucketBy(2, "first").saveAsTable("fogbox")

  val d2 = spark.sql("select * from fogbox")
  val first: Row = d2.orderBy($"first").first
  System.out.println("first:" + first);
  assert(first == Row("bob", "y", 2))
  println("select * from fogbox")
  d2.show()
  assert(spark.sql("select distinct first from fogbox").collect().toList.size == 4)

  val pdf3 = List(Parson("axel", "x", 9)).toDF()
  pdf3.write.insertInto("fogbox")
  spark.sql("select distinct first from fogbox").show()

  assert(spark.sql("select distinct first from fogbox").collect().toList.size == 5)  // one more than before




  // DDL formatted schema - you need a schema if you want to convert an RDD of Row to DataFrame
  //
  import org.apache.spark.sql.types._

  val rdd = spark.sparkContext.parallelize(List(Row("joe", 9)))


  // The statement below won't work, 'cause you need a schema when dealing with rows
  // spark.createDataFrame( rdd)

  val schema = StructType.fromDDL("name STRING, rank INT")
  val frame = spark.createDataFrame(rdd, schema).select($"rank" + 1)
  frame.show()
  assert(frame.collect().toList.head.getAs[Integer](0) == 10)

  val sc2 = StructType(List(StructField("name", StringType), StructField("rank", IntegerType)))
  assert(sc2 == schema)


  // RDD's of product are directly convertible to datasets
  case class Foo(x: Integer)
  val x: Dataset[Foo] = spark.sparkContext.parallelize(List(Foo(2))).toDS()

}


