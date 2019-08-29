package com.lackey.spark.examples.sqlfuns
import scala.collection.mutable

// Solution is given in two parts so that we can more easily compute the complexity. The first step
// findPairsAllowingDups involves computing the list of all pairs of numbers in array that would sum to
// k, allowing dups.
//
//  Note for first step we make two passes through the array where our operations are looking up and updating
//  things in a map (constant time operations as long as the map is sparse enough so collisions are rare), and
//   adding to the 'companions' list. Each addition to companions list is constant time since it should be a 'cons'
//  operation, and as long as there are no dups in our list the number of add's to 'companions' is only one.
//  Now.. if we allow dups in our input then this complexity argument will not hold, and we'd have to further
//  optimize.  Assuming no dups, then we have complexity N for first phase
//
//  Now, the complexity of the 'eliminateDups' phase is at most order N because our input array has to have less
//  than N pairs. We make one pass through the input array
//  to do the map operation to  ensure lowest value element is first in pair. Then we make another pass through
//  all our matches and update the pairs map. We know there can't be more than N updates to this map. So
//  complexity of 'eliminateDups' phase is at most order N.
//
//   We have 2 phases, both of order N, so we can state that the whole algorithm is order N (assuming no dups).
//
object AllPairsThatSumToK extends App {

  import scala.collection.mutable.ArrayBuffer

  def findPairsAllowingDups(k: Int, array: Seq[Int]): Seq[(Int,Int)] = {

    val pairs = ArrayBuffer[(Int,Int)]()
    val matches = mutable.Map[Int,ArrayBuffer[Int]]()

    // First pass through array computes match map. Given a number in the array -- call it 'diff' -- we
    // want to find all the array positions of other numbers in the array which, when added to 'diff' == k
    array.indices.foreach { i =>
      val diff: Int = k - array(i)
      val indices   = matches.getOrElse(diff, new ArrayBuffer[Int]())
      indices.append(i)
      matches.update(diff, indices)
    }

    // Second pass through where for each element 'i' in array we check the map to see if  there are any 'companion'
    // numbers which when added to 'i' would give 'k
    array.indices.foreach { i =>
      // each companion 'c' in 'companions' is an index into array such that array(c) + array(i) = k
      // companions may be empty if for the array(i) no other element in the array will sum to k when added to array(i)
      val companions = matches.getOrElse(array(i), Seq[Int]())
      for (c <- companions) {
        pairs +=  ( (array(i), array(c))  )
      }
    }

    pairs.toList
  }


  def eliminateDups(array: Seq[(Int,Int)]) : Seq[(Int,Int)] = {
    val pairs = mutable.Map[Int,Int]()

    // ensure lowest value element is first in pair
    val eachPairLowToHigh = array.map{case (i,j) => if (i>j) (j,i) else (i,j)}

    eachPairLowToHigh.foreach{ case(i,j) =>  pairs.update(i,j) }
    pairs.toList
  }

  def findPairs(k: Int, array: Seq[Int]): Seq[(Int,Int)] = eliminateDups(findPairsAllowingDups(k, array))

  assert( findPairs(2, Seq(0, 2, -1)) == List( (0,2)) )
  assert( findPairs(2, Seq(4, 0, 2, -2, -1)) == List((-2,4), (0,2)) )
  assert( findPairs(2, Seq[Int]()) == List[Int]())
  assert( findPairs(2, Seq(100, 500, 9)) ==  List[Int]())

  new java.text.SimpleDateFormat("yyyy-MM-dd").parse("1970-01-01").getTime / 1000

}


object TimeTricks {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions._

    val spark: SparkSession = SparkSession.builder() .master("local[*]") .appName("timefoo") .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    import java.sql.Timestamp

    def timeTricks(timezone: String): Unit =  {
      val df2 = List("1970-01-01").toDF("timestr"). // can use to_timestamp even without time parts !
        withColumn("timestamp", to_timestamp('timestr, "yyyy-MM-dd")).
        withColumn("localizedTimestamp", from_utc_timestamp('timestamp, timezone)).
        withColumn("weekday", date_format($"localizedTimestamp", "EEEE"))
      
      df2.explain(true)

      val row = df2.first()
      println("with timezone: " + timezone)
      df2.show()
      val (timestamp, weekday) = (row.getAs[Timestamp]("localizedTimestamp"), row.getAs[String]("weekday"))

      timezone match {
        case "UTC" =>
          assert(timestamp ==  Timestamp.valueOf("1970-01-01 00:00:00")  && weekday == "Thursday")
        case "PST" | "GMT-8" | "America/Los_Angeles"  =>
          assert(timestamp ==  Timestamp.valueOf("1969-12-31 16:00:00")  && weekday == "Wednesday")
        case  "Asia/Tokyo" =>
          assert(timestamp ==  Timestamp.valueOf("1970-01-01 09:00:00")  && weekday == "Thursday")
      }
    }

    timeTricks("UTC")
    timeTricks("PST")
    timeTricks("GMT-8")
    timeTricks("Asia/Tokyo")
    timeTricks("America/Los_Angeles")
  }
}

//  given what we see in TimeTricks, above, it seems like we should be able to correct the skewed
// display of our time windows (by 8 hours which is PST's offset from GMT using from_utc_timestamp... That
// is what worked above. But it turns out that from_utc_timestamp shifts things in the opposite direction...
// not sure what. So the pragmatic working solution is to go with to_utc_timestamp ... this shifts things correctly
// so the displayed window start and end times correctly bracket the input data.
object UnexpectedResultsWhenUsingGroupBy {
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkByExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val timezone = "PST"

    val df =
      List( ("1970-01-01", 4) ).toDF("tsString", "count").
        withColumn("timestamp", to_timestamp($"tsString", "yyyy-MM-dd"))

    val grouped = df.groupBy(window($"timestamp", "1 day", "1 day").as("date_window")).count()

    // The date window incorrectly shifts by 8 hours (my timezone -- PST's --- offset from GMT)
    grouped.show(false)

    // So...let's try applying what worked for us in TimeTricks to get things shifted back...
    val displayShiftedInWrongDirection =
      grouped.
        withColumn("windowStart", from_utc_timestamp($"date_window.start", timezone)).
        withColumn("windowEnd", from_utc_timestamp($"date_window.end", timezone))

    displayShiftedInWrongDirection.show(false)    // nope !   doesn't work.. shifted even further away from correct !

    val displayShiftedInCorrectDirection =
      grouped.
        withColumn("windowStart", to_utc_timestamp($"date_window.start", timezone)).
        withColumn("windowEnd", to_utc_timestamp($"date_window.end", timezone))

    displayShiftedInCorrectDirection.show(false)      // now this is displaying correctly
  }
}



object UnexpectedResultsWhenUsingGroupBy2 extends App  {
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("bigfootbigfoot")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._


  // For each five day period, group by place, and guest, and give the number of visits
  // each guest made to 'place', where we count multiple visits by a given guest to the same place on the
  // same date as one unique visit by that guest to that place .
  //
  // Expected
  //
  //   window                       place         guest   count
  //   2001-01-01- 2001-01-05       ny            joe     2
  //                                la            joe     1
  //   2001-01-06- 2001-01-10       ny            joe     1
  //                                              bob     1
  //
  val df = List(
    ("ny","joe", "2001-01-01", 0),
    ("ny","joe", "2001-01-01", 1),

    ("ny","joe", "2001-01-02", 0),
    ("ny","joe", "2001-01-02", 1),


    ("la","joe", "2001-01-02", 0),
    ("la","joe", "2001-01-02", 1),


    ("ny","bob", "2001-01-06", 1),
    ("ny","joe", "2001-01-07", 0)
  ).toDF("place", "guest", "date", "visitId")


  val distinctDf = df.
    select($"place", $"guest", $"date").distinct()
  distinctDf.printSchema()

  val windowedDf = distinctDf .
    withColumn("ts", to_timestamp($"date", "yyyy-MM-dd")).
    groupBy(window($"ts", "5 days"), $"place", $"guest").
    agg(count($"guest").as("visits")).
    orderBy($"window")

  windowedDf.show(false)    // This is wrong ... bad result is below..  Windows are shifted 8 hours
                            // AND our windows don't start on the first of the month

  // Let's get our windows shifted so that they start on the first of the month

  val windowedDf2 = distinctDf .
    withColumn("ts", to_timestamp($"date", "yyyy-MM-dd")).
    groupBy(window($"ts", "5 days", "5 days", "4 days"), $"place", $"guest").
    agg(count($"guest").as("visits")).
    orderBy($"window")

  windowedDf2.show(false)    // This is wrong ... bad result is below..  Windows are shifted 8 hours


  val windowedDf3 = distinctDf .
    withColumn("ts", to_timestamp($"date", "yyyy-MM-dd")).
    groupBy(window($"ts", "5 days", "5 days", "4 days"), $"place", $"guest").
    agg(count($"guest").as("visits")).
    orderBy($"window").
    withColumn("windowStart", to_utc_timestamp($"window.start", "PST")).
    withColumn("windowEnd", to_utc_timestamp($"window.end", "PST"))
  
  windowedDf3.show(false)  // Better !   we have eliminated the 8 hour shift due to PST.. but we need to tweak days


  val windowedDf4 = distinctDf .
    withColumn("ts", to_timestamp($"date", "yyyy-MM-dd")).
    groupBy(window($"ts", "5 days", "5 days", "3 days"), $"place", $"guest").
    agg(count($"guest").as("visits")).
    orderBy($"window").
    withColumn("windowStart", to_utc_timestamp($"window.start", "PST")).
    withColumn("windowEnd", to_utc_timestamp($"window.end", "PST"))

  windowedDf4.show(false)  








}
