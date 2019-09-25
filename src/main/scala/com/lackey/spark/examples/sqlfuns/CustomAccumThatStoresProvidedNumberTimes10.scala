package com.lackey.spark.examples.sqlfuns

import java.io.PrintWriter

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.language.implicitConversions


object CustomAccumThatStoresProvidedNumberTimes10 extends App {

  val spark = org.apache.spark.sql.SparkSession.builder().appName("simple").master("local[*]").getOrCreate()

  import org.apache.spark.util.AccumulatorV2

  class MyAccum extends AccumulatorV2[Int, Int]() {
    var accum = 0

    def add(v: Int): Unit = {
      accum = v * 10
    }

    def copy(): org.apache.spark.util.AccumulatorV2[Int, Int] = {
      val copy = new MyAccum()
      copy.accum = this.accum
      copy
    }

    def isZero: Boolean = accum == 0

    def merge(other: org.apache.spark.util.AccumulatorV2[Int, Int]): Unit = accum += other.value

    def reset(): Unit = accum = 0

    def value: Int = accum
  }

  val acc = new MyAccum()
  spark.sparkContext.register(acc, "My Accum2")

  val ints = spark.sparkContext.parallelize(List(1, 2, 3))


  ints.foreach { i => acc.add(i) }

  println("value of accum is: " + acc.value)
}
//  This shows how when we read in a json file without a schema the _corrupt_record column gets automatically
//  added if there is a record with syntactically invalid JSON.
object DemoOfHowReadingJsonWithoutSchemaWillFlagRecsWithInvalidJsonSyntax extends App {

  def createInputFile(json: String): String = {
    import java.io.PrintWriter

    import scala.sys.process._

    val outfile = "/tmp/junk.json"
    s"rm -rf $outfile ".!

    new PrintWriter(outfile) {
      write(json);
      close()
    }
    outfile
  }


  val sparkSession = org.apache.spark.sql.SparkSession.builder().appName("simple").master("local[*]").getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  import sparkSession.implicits._

  def countValidAndInvalidJsonRecords(jsonContents: String) = {
    val jFileName = createInputFile(jsonContents)
    val fromFileDS = sparkSession.read.json(jFileName)


    val good = sparkSession.sparkContext.longAccumulator
    val bad = sparkSession.sparkContext.longAccumulator

    if (fromFileDS.columns.contains("_corrupt_record")) {

      val count = fromFileDS.cache().filter($"_corrupt_record".isNotNull).count()
      System.out.println("here is count using 'official' recommended approach:" + count)

      fromFileDS.foreach { row =>
        if (row.getAs[Any]("_corrupt_record") != null)
          bad.add(1)
        else
          good.add(1)
      }
      (good.count, bad.count) // This approach is silly.. but seems to work... good accumulator practice !
    } else {
      (fromFileDS.count(), 0L)
    }
  }

  val goodRec = """{ "a": 1, "b": 2 }"""
  val badRec = """!"a": 1, "b": 2 }"""      // invalid JSON syntax

  assert(countValidAndInvalidJsonRecords(s"$goodRec\n$goodRec") == (2, 0))
  assert(countValidAndInvalidJsonRecords(s"$goodRec\n$badRec") == (1, 1))
}


object ArrayHandlingFunctionsIntroducedInSpark24 extends App {


  def createInputFile(json: String): String = {
    import scala.sys.process._

    val outfile = "/tmp/junk.json"
    s"rm -rf $outfile ".!

    new PrintWriter(outfile) {
      write(json);
      close()
    }
    outfile
  }


  val sparkSession = SparkSession.builder().appName("simple").master("local[*]").getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  import sparkSession.implicits._

  // select average score from array of scores
  //
  val avDF = Seq(
    ("joe", Seq(10, 20, 0)),
    ("moe", Seq(8, 2, 2)),
  ).toDF("id", "scoreList")

  avDF.createTempView("scores")

  sparkSession.sql(
    "SELECT (aggregate(scoreList, 0, (acc, x) -> acc + x)) / size(scoreList) as avgScore from scores").show()
  // RESULT:
  //+--------+
  //|avgScore|
  //+--------+
  //|    10.0|
  //|     4.0|
  //+--------+


  // Next result illustrates that the 'array' function uses zero based offsets
  avDF.select(array_contains($"scoreList", 8), array(lit(100), $"scoreList" (1))).show()
  // RESULT:
  //+----------------------------+------------------------+
  //|array_contains(scoreList, 3)|array(100, scoreList[1])|
  //+----------------------------+------------------------+
  //|                       false|               [100, 20]|
  //|                       false|                [100, 2]|
  //+----------------------------+------------------------+


  avDF.select(array_remove($"scoreList", 2).as("newArray")).show()

  // Sort an array of structs where the struct initially doesn't have the key in the correct place, but we move it there
  // The key in this case is the course name
  val structDF: Dataset[(String, Seq[(Int, Int, String)])] = Seq(
    ("joe",
      Seq(
        (90, 50, "math"),
        (44, 40, "reading"),
        (12, 30, "history"),
        (20, 18, "gym")
      )
    )
  ).toDS()

  // Now sort the array of structs after moving the id to the first element position
  // (which is what sort uses for the order).  Note that the scores are not sorted. They are left as is.
  //
  val rearanged = structDF.map { case (name: String, structs: Seq[(Int, Int, String)]) =>
    val idFirstStructs = structs.map { case (best: Int, worst: Int, id: String) =>
      (id, best, worst)
    }

    (name, idFirstStructs)
  }

  rearanged.select($"_1", array_sort($"_2").as("testResults")).show(truncate = false)



}

object EncoderExample extends App {
  val spark = SparkSession.builder().appName("simple").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  FileInputHelper.setup

  // Try reading data from a JSON file while applying a schema to it.
  // Note that 'jsonBad2'  is just like 'goodJson', w/ the exception of one of the int fields having the
  // value "NOT_INT", instead of being a string of digits (which would could be converted to a valid integer). If we
  // were to convert 'NOT_INT' to a number like '5', then we would see non-null values for  _1 and _2 in the output.
  // Note that the illegal Integer value NOT_INT that we introduced also messes up column _1 (which becomes null.)
  // The whole record is thrown away.  This also happens with the 'toe' record, which has the legal (per Json),
  // non-integer value "22" as the first attribute of the first element of the array of structs.   But note, "22" is an
  // invalid value per the Rec schema that we use to parse the JSON, however the "toe" record is syntactically
  // correct JSON.
  //
  val goodJson =
  """{"_1":"joe","_2":[{"_1":10,"_2":50,"_3":"math"},{"_1":14,"_2":40,"_3":"reading"},{"_1":12,"_2":30,"_3":"history"},{"_1":19,"_2":20,"_3":"gym"}]}"""
  val jsonBad1 =
    """{"_2":[{"_1":10,"_2":50,"_3":"lath"},{"_1":14,"_2":40,"_3":"breeding"},{"_1":12,"_2":30,"_3":"history"},{"_1":19,"_2":20,"_3":"gym"}]}"""
  val jsonBad2 =
    """{"_1":"hoe","_2":[{"_1":NOT_INT,"_2":50,"_3":"math"},{"_1":14,"_2":40,"_3":"reading"},{"_1":12,"_2":30,"_3":"history"},{"_1":19,"_2":20,"_3":"gym"}]}"""
  val jsonBad3 =
    """{"_1":"toe","_2":[{"_1":"22","_2":50,"_3":"math"},{"_1":14,"_2":40,"_3":"reading"},{"_1":12,"_2":30,"_3":"history"},{"_1":19,"_2":20,"_3":"gym"}]}"""
  val fileContents = Seq(goodJson, jsonBad1, jsonBad2, jsonBad3).mkString("\n")
  val filePath = "/tmp/input/json.json"

  System.out.println(s"json: $fileContents")
  val jfile =    FileInputHelper.createInputFile(fileContents, filePath)


  type Rec = (String, Seq[(Int, Int, String)])
  val schema = Encoders.product[Rec].schema

  val jdf = spark.read.schema(schema).json(filePath)
  jdf.show(false)

}

// To flag _corrupt_record's you need to explicitly add a column of that name to the schema. However, this
// feature fails to identify certain corrupt record scenarios. This is illustrated below in the test with {"mouse": 3}
// at the tail end of the input.  This 'mouse' record has a field, 'mouse', that is not expected by the schema, and
// even so spark does not consider this record to be corrupt.  Also, this record lacks the 'field' attribute,
// which again would seem to invalidate the record, but even though our schema definition says this field is
// required, Spark treats the nullable = false as advisory. So this record is not flagged as a _corrupt_record.
//
// There is one other twist to _corrupt_record feature... see comment in
// DemoOfHowReadingJsonWithoutSchemaWillFlagRecsWithInvalidJsonSyntax
object MoreCorruptRecordTest extends App {

  FileInputHelper.setup

  val spark = SparkSession.builder().appName("simple").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  import org.apache.spark.sql.types._

  def runTest(input: String, knownBadRec: String, addCorruptRecordCol : Boolean = true): Long = {
    val schema =
      if (addCorruptRecordCol) {
        new StructType() .add("field", ByteType, nullable = false) .add("_corrupt_record", StringType)
      } else {
        new StructType() .add("field", ByteType, nullable = false)
      }

    val file = "/tmp/sample.json"

    FileInputHelper.createInputFile(input, file)

    val df = spark.read.schema(schema).json(file)
    df.show(false)

    val count = spark.sparkContext.longAccumulator("corruptCount")
    count.reset()
    if (df.columns.contains("_corrupt_record")) {
      df.foreach{ row =>
        val corrupt = row.getAs[String]("_corrupt_record")
        if (null != corrupt) {
          count.add(1L)
          assert(corrupt == knownBadRec)
        }
      }
    }
    count.value
  }

  // The next two tests illustrate that we must explicitly add a _corrupt_record  column to the schema
  // if we want our corrupt record flag. In the second instance our corrupt count is 0, even though we
  // have the same known bad record in both cases.  The point of the second test is to illustrate that
  // the we don't explicitly add a _corrupt_record column, Spark does not add one automatically.
  val input2 =
    """{"field": 1}
      |{"field": 2}
      |{"field": "3"}""".stripMargin
  assert(runTest(input2, """{"field": "3"}""") == 1L)

  val input2b =
    """{"field": 1}
      |{"field": 2}
      |{"field": "3"}""".stripMargin
  assert(runTest(input2b, """{"field": "3"}""", false) == 0L)  // _corrupt_record not added automatically

  // Missing 'field' attribute & superfluous 'mouse' attribute fails to cause record to be considered corrupt
  val input1 =
    """{"field": 1}
      |{"field": 2}
      |{"mouse": 3}""".stripMargin
  assert(runTest(input1, """{"mouse": 3}""") == 0L)
}

object FileInputHelper {
  def setup: Int = {
    import scala.sys.process._
    "rm -rf /tmp/input".!
    "rm -rf /tmp/ff".!
    "rm -rf /tmp/badRecordsPath".!
    "mkdir  /tmp/input".!
  }

  def createInputFile(json: String, outFilePath: String): String = {
    import java.io.PrintWriter

    import scala.sys.process._

    s"rm -rf $outFilePath ".!

    new PrintWriter(outFilePath) {
      write(json);
      close()
    }
    outFilePath
  }
}


