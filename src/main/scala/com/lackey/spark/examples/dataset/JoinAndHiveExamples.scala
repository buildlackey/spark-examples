package com.lackey.spark.examples.dataset

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType




object DateFun extends App {
  val conf = new SparkConf().setAppName("blah").setMaster("local").set("spark.sql.shuffle.partitions", "2")
  val sparkSession = SparkSession.builder
    .config(conf)
    .getOrCreate()
  val spark = sparkSession

  import spark.implicits._

  sparkSession.sparkContext.setLogLevel("ERROR")

  val oneDay = 1000 /* millisecs */ * 60 /* secs */ * 60 /* secs per hour */ * 24 /* hrs per day */

  val df = Seq(
    oneDay,
    0).toDF("timestamp")

  //df.withColumn($"timestamp" / lit(0))


  // Next example converts a string specifying the locale independent date of the start of the unix epoch (1970-01-01)
  // to a time stamp equivalent to whatever GMT time was at midnight IN LOCAL TIME ZONE of that (start of epoch) date.
  //
  val dateString = "1970-01-01" // locale independent date of of the start of the unix epoch
  val df2 = Seq(dateString).toDF("date")
  val df3 = df2.withColumn("ts", unix_timestamp($"date", "yyyy-MM-dd"))
  val first = df3.select($"ts").first()

  import java.time._
  import java.time.format.DateTimeFormatter

  val ld: LocalDate = LocalDate.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  val localDateInGreenwichAt_ld: ZonedDateTime = ZonedDateTime.of(ld.atStartOfDay(), ZoneId.systemDefault())
  val timeStampInGreenwhichAt_ld = localDateInGreenwichAt_ld.toInstant().getEpochSecond

  assert(first.getAs[Long](0) == timeStampInGreenwhichAt_ld) // in PST timezone this value == 28800

  // Can we do date functions on the column 'ts' ?   No !
  // If we try this:
  //
  //  val dayDf = df3.select(dayofmonth($"ts"))
  //  df3.show()
  //
  // We get this:   AnalysisException: cannot resolve 'dayofmonth(`ts`)' due to data type mismatch:
  //                argument 1 requires date type, however, '`ts`' is of bigint type.;;
  //
  // This is the way we need to do it (via cast):

  import org.apache.spark.sql.types._

  val dfAsDate = df3.withColumn("asDate", $"ts".cast(TimestampType)).select(dayofmonth($"asDate"))
  assert(dfAsDate.first().getAs[Integer](0) == 1)


  // Calculate hours difference between two formatted date/time strings with and withOUT the leading zero
  def timefoo(time1: String, time2: String): Unit = {
    val fmt = "MM-dd-yyyy HH:mm"
    val t1 = time1
    val t2 = time2
    val tdf1 = List((t1, t2)).toDF("timestamp1", "timestamp2")

    val tdf3 = tdf1.
      withColumn("h1", hour(unix_timestamp($"timestamp1", fmt).cast(TimestampType))).
      withColumn("h2", hour(unix_timestamp($"timestamp2", fmt).cast(TimestampType))).
      select($"h2" - $"h1")
    assert(tdf3.first.getAs[Int](0) == 1)

    // Even easier way than the one immediately above
    //
    val tdf4 = tdf1.
      withColumn("h1", hour(to_timestamp($"timestamp1", fmt))).
      withColumn("h2", hour(to_timestamp($"timestamp2", fmt))).
      select($"h2" - $"h1")
    assert(tdf4.first.getAs[Int](0) == 1)
  }

  timefoo("2-02-2001 01:00", "02-2-2001 02:00")
  timefoo("02-02-2001 01:00", "2-02-2001 02:00")


  // Make sure we can sort dates in proper order, which is not alphabetically !  And handle nulls so they are last !
  //
  val datesAndEventNames = List( // List of dates one day before the birth day of each person. convert to real b-day !
    ("07-22-2008", "moe"),
    ("cant-parse-2008", "null"),
    ("01-22-2009", "last"),
    ("03-22-2001", "first"),
    ("09-22-2002", "joe")
  ).toDF("date", "name")

  val bdaysDF = datesAndEventNames.
    withColumn("parsedDate", to_date($"date", "MM-dd-yyyy")).
    select(
      date_add($"parsedDate", 1).   // add 1 to convert to actual birthday
        as("dateOfBirth"), $"name").
    orderBy($"dateOfBirth".asc_nulls_last)
  bdaysDF.show()
  // RESULT
  //+-----------+-----+
  //|dateOfBirth| name|
  //+-----------+-----+
  //| 2001-03-23|first|
  //| 2002-09-23|  joe|
  //| 2008-07-23|  moe|
  //| 2009-01-23| last|
  //|       null| null|
  //+-----------+-----+

  // Calculate days difference between two formatted date/time strings
  def datefoo(): Unit = {
    val fmt = "MM-dd-yyyy"
    val t1 = "01-01-2002"
    val t2 = "12-31-2001"
    val tdf1 = List((t1, t2)).toDF("date1", "date2")
    val tdf2 = tdf1.
      withColumn("date1", to_date($"date1", fmt)).
      withColumn("date2", to_date($"date2", fmt)).
      select(datediff($"date1", $"date2"))
    assert(tdf2.first.getAs[Int](0) == 1)

  }

  datefoo()
}

object JoinFun extends App {

  val conf = new SparkConf().setAppName("blah").setMaster("local").set("spark.sql.shuffle.partitions", "2")
  val sparkSession = SparkSession.builder
    .config(conf)
    .getOrCreate()
  val spark = sparkSession

  import spark.implicits._

  sparkSession.sparkContext.setLogLevel("ERROR")

  // Include nulls in join condition: https://stackoverflow.com/questions/41728762/including-null-values-in-an-apache-spark-join
  val numbersDf = Seq(
    "123",
    "456",
    null,
    ""
  ).toDF("numbers")

  val lettersDf = Seq(
    ("123", "abc"),
    ("456", "def"),
    (null, "zzz"),
    ("", "hhh")
  ).toDF("numbers", "letters")

  val joinedDf = numbersDf.join(lettersDf, numbersDf.col("numbers") <=> lettersDf.col("numbers"))
  joinedDf.show()
  // RESULT
  //+-------+-------+-------+
  //|numbers|numbers|letters|
  //+-------+-------+-------+
  //|    123|    123|    abc|
  //|    456|    456|    def|
  //|   null|   null|    zzz|
  //|       |       |    hhh|
  //+-------+-------+-------+

  // Some ways to eliminate the duplicated column 'numbers' used as join condition
  val j2 =
    numbersDf.
      join(
        lettersDf,
        numbersDf.col("numbers") <=> lettersDf.col("numbers")).drop(lettersDf.col("numbers"))
  j2.show()
  // RESULT: same as RESULT ONE, above, but without numbers column appearing twice in output

  // For equi-joins there exist a special shortcut syntax which takes either a sequence of strings or single string
  // But this syntax will not allow you to include the null columnn matches in the join results
  val j3 = numbersDf.join(lettersDf, Seq("numbers"))
  j3.show()
  val j4 = numbersDf.join(lettersDf, "numbers")
  j4.show()
  // RESULT: same as RESULT ONE, above, but without dups

  // Exercise more specific control when there are multiple columns duplicated
  val numbersDf2 = Seq(
    ("123", 2),
    ("456", 28),
    (null, 29),
    ("", 22)
  ).toDF("numbers", "dup")

  val lettersDf2 = Seq(
    ("123", "abc", 2),
    ("456", "def", 4),
    (null, "zzz", 42),
    ("", "hhh", 44)
  ).toDF("numbers", "letters", "dup")
  val j5 =
    numbersDf2.alias("n").
      join(
        lettersDf2.alias("l"),
        Seq("numbers")).
      select($"n.numbers", $"letters", $"l.dup")
  j5.show()
  //RESULT:
  //+-------+-------+---+
  //|numbers|letters|dup|
  //+-------+-------+---+
  //|    123|    abc|  2|
  //|    456|    def|  4|
  //|       |    hhh| 44|
  //+-------+-------+---+


  // Or, use aliases to drop the column you don't want
  val j6 = numbersDf2.alias("n").join(lettersDf2.alias("l"), Seq("numbers")).drop($"n.dup")
  j6.show()
  // RESULT:
  //+-------+-------+---+
  //|numbers|letters|dup|
  //+-------+-------+---+
  //|    123|    abc|  2|
  //|    456|    def|  4|
  //|       |    hhh| 44|
  //+-------+-------+---+
}

object WriteTableAndVerifyItExistsInHive extends App {

  val spark = SparkSession
    .builder()
    .appName("interfacing spark sql to hive metastore without configuration file")
    .config("hive.metastore.uris", "thrift://localhost:9083") // replace with your hivemetastore service's thrift url
    .enableHiveSupport() // don't forget to enable hive support
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // create an arbitrary frame
  val frame = Seq(("one", 1), ("two", 2), ("three", 3)).toDF("word", "count")
  // see the frame created
  frame.show()
  /**
    * +-----+-----+
    * | word|count|
    * +-----+-----+
    * |  one|    1|
    * |  two|    2|
    * |three|    3|
    * +-----+-----+
    */
  // write the frame
  frame.write.mode("overwrite").saveAsTable("t4")


}
