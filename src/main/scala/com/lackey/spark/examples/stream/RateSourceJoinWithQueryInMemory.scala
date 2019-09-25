package com.lackey.spark.examples.stream

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.IntegerType

object RateSourceJoinWithQueryInMemory extends App {

  val spark = SparkSession.builder().appName("foo").master("local[*]").getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  val keysForComplete =
    Array("dog", "ox", "pig", "duck", "goat", "cat", "sheep", "eel", "bird", "mouse", "bear")
  val keysForEveryThree =
    Array("red", "blue", "green", "pink", "orange", "black", "white", "beige", "purple", "gray", "brown")

  val completeDf  =
    spark.readStream. format("rate").load().
      withWatermark("timestamp", "10 seconds").withColumn("valueMod10", expr("mod(value,10) + 1").cast(IntegerType)).
      withColumn("key",  element_at(lit(keysForComplete), $"valueMod10"))

  val everyThreeDf  =
    spark.readStream.format("rate").load().
      withWatermark("timestamp", "10 seconds").
      filter("mod(value, 3) = 0").withColumn("valueMod10", expr("mod(value,10) + 1").cast(IntegerType)).
      withColumn("key",  element_at(lit(keysForEveryThree), $"valueMod10"))

  val joined = completeDf.join(everyThreeDf, "valueMod10")

  val x: Thread = new Thread () {
    override def run(): Unit = {
      while(true) {
        Thread.sleep(5 * 1000)
        spark.sql("select * from mem").show()
      }
    }
  }
  x.start()


  joined.
    writeStream.
    format("memory").outputMode("append").
    queryName("mem").
    option("truncate","false").start().awaitTermination()


}
