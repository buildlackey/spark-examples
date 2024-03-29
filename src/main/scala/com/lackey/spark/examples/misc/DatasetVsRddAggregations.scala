package com.lackey.spark.examples.misc


import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}




object DatasetAggs extends App {

  val sparkSession = SparkSession.builder
    .master("local")
    .appName("example")
    .getOrCreate()


  sparkSession.sparkContext.setLogLevel("ERROR")

  import sparkSession.implicits._
  import collection.JavaConverters._

  /*
   Calculate total orders per customer, per day
   Calculate total revenue perday and per order
   Calculate total and average revenue for each date. -
  */

  val orders: DataFrame =
    List(
      (0, "1/2/2011", "tim", "pending"),
      (1, "1/2/2011", "tim", "pending"),
      (2, "1/2/2011", "bob", "pending"),
      (3, "2/2/2011", "tim", "pending")
    ).toDF("orderId", "date", "cust", "status")

  // ( order_item_order_id ,  order_item_id ,  order_item_product_id, order_item_quantity,    order_ item_product_price)
  val orderItems: DataFrame =
    List(
      (0, 1, "beer", 2, 20),
      (0, 2, "nuts", 3, 10),
      (1, 1, "tacos", 1, 40),
      (2, 1, "nuts", 10, 10),
      (2, 2, "tacos", 1, 30),
      (3, 1, "nuts", 1, 10)
    ).toDF("orderId", "itemId", "productId", "orderQuantity", "itemPrice")

  /*
   |-- orderId: integer (nullable = false)
   |-- date: string (nullable = true)
   |-- cust: string (nullable = true)
   |-- status: string (nullable = true)
   |-- orderId: integer (nullable = false)
   |-- itemId: integer (nullable = false)
   |-- productId: string (nullable = true)
   |-- orderQuantity: integer (nullable = false)
   |-- itemPrice: integer (nullable = false)
   */
  private val ordersWithItems: DataFrame =
    orders.as('a).join(orderItems.as('b), $"a.orderId" === $"b.orderId")



  // REDUNDANT ... delete !
  println("total revenue per day AGAIN")
  val orderWithItems =
    orders.join(orderItems, "orderId")

  val revPerDay =
    orderWithItems.
      withColumn("revForOrderItem" ,$"orderQuantity" * $"itemPrice").
      withColumn("date" , to_date($"date", "MM/dd/yyyy")).
      groupBy($"date").
      agg(sum($"revForOrderItem")).
      orderBy($"date")

  revPerDay.show(false)


  println("total orders by day")
  ordersWithItems
    .groupBy("date").agg(sum($"b.orderQuantity" * $"b.itemPrice"))
    .show()

  println("total revenue per order")
  ordersWithItems
    .groupBy("a.orderId").agg(sum($"b.orderQuantity" * $"b.itemPrice"))
    .show()

  println("total revenue per day ")
  ordersWithItems
    .groupBy("date")
    .agg(
      sum($"b.orderQuantity" * $"b.itemPrice").as("revenue"),
      countDistinct($"b.orderId").as("numOrders"))
    .withColumn("avgPerOrder", $"revenue" / $"numOrders")
    .show()


  println("total orders per customer,per day")
  orders
    .groupBy("cust", "date").agg(count($"orderId"))
    .show()
}


object OrdersExampleWithRddGroupBy extends App {

  val spark = SparkSession
    .builder()
    .appName("interfacing spark sql to hive metastore without configuration file")
    .config("hive.metastore.uris", "thrift://localhost:9083") // replace with your hivemetastore service's thrift url
    .enableHiveSupport() // don't forget to enable hive support
    .master("local[*]")
    .getOrCreate()


  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  import collection.JavaConverters._


  val orderSchema = StructType(
    List(
      StructField("orderId", IntegerType),
      StructField("date", StringType),
      StructField("cust", StringType),
      StructField("status", StringType)
    )
  )


  val rawDf =
    spark.read.
      option("delimiter", ",").
      schema(orderSchema).
      csv("hdfs://quickstart.cloudera:8020/tmp/orders.csv")

  val ordersDf = rawDf.withColumn("date", to_date($"date", "MM/dd/yyyy"))

  // ( order_item_order_id ,  order_item_id ,  order_item_product_id, order_item_quantity,    order_ item_product_price)
  val orderItems: DataFrame =
    List(
      (0, 1, "beer", 2, 20),
      (0, 2, "nuts", 3, 10),
      (1, 1, "tacos", 1, 40),
      (2, 1, "nuts", 10, 10),
      (2, 2, "tacos", 1, 30),
      (3, 1, "nuts", 1, 10)
    ).toDF("orderId", "itemId", "productId", "orderQuantity", "itemPrice")


  val orderWithRev = orderItems.withColumn("revForItem", $"orderQuantity" * $"itemPrice")

  val joinedDf = ordersDf.join(orderWithRev, "orderId")

  joinedDf.groupBy($"date", $"orderId").agg(sum($"revForItem")).
    orderBy($"date", $"orderId").
    show(false)


  case class Order(orderId: Int, date: Date, cust: String, status: String) {}
  case class OrderItem(orderId: Int,  itemId: Int, productId: String, orderQuantity: Int, itemPrice: Int)

  val dateFmt = new SimpleDateFormat("MM/dd/yyyy")

  val orderItemsKeyed: RDD[(Int, OrderItem)] = orderItems.as[(Int, Int, String, Int, Int)].map{ tup =>
    val orderId = tup._1.toInt
    val itemId = tup._2.toInt
    val productId = tup._3
    val orderQuantity = tup._4.toInt
    val itemPrice= tup._5.toInt
    (orderId, OrderItem(orderId, itemId, productId, orderQuantity, itemPrice))
  }.rdd

  val textRdd = spark.sparkContext.textFile("hdfs://quickstart.cloudera:8020/tmp/orders.csv")

  val ordersKeyed: RDD[(Int, Order)] =
    textRdd.map(_.split(",")).
      map {
        arr =>
          val orderId = arr(0).toInt
          val dateStr = arr(1)
          val cust = arr(2)
          val status = arr(3)
          val date = dateFmt.parse(dateStr.replaceAll("\"", ""))
          (orderId, Order(orderId, date, cust, status))
      }

  val joinedRdd: RDD[(Int, (Order, OrderItem))] = ordersKeyed.join(orderItemsKeyed)
  val orderItemsKeyedByDateOrderId: RDD[((Date, Int), Int)] =
    joinedRdd.map{case (orderId, (order, orderItem))  =>
      ((order.date, orderId), orderItem.itemPrice * orderItem.orderQuantity)
    }

  val groupedRdd: Array[((Date, Int), Iterable[Int])] = orderItemsKeyedByDateOrderId.groupByKey().collect()

  groupedRdd.foreach{ case ((date,id), iter) =>
    println(s"$date/$id: ${iter.sum}")
  }
}

object RddAggsMoreEfficientThanGroupBy extends App {

  case class Line(orderId: Int, cust: String, day: String, totForItem: Int)

  val sparkSession = SparkSession.builder
    .master("local")
    .appName("example")
    .getOrCreate()


  sparkSession.sparkContext.setLogLevel("ERROR")

  import org.apache.spark.rdd.RDD
  import sparkSession.implicits._
  import collection.JavaConverters._

  /*
   Calculate total orders per customer, per day
   Calculate total revenue perday and per order
   Calculate total and average revenue for each date. - combineByKey
  */
  val orders = sparkSession.sparkContext.parallelize(
    List(
      (0, "1/2/2011", "tim", "pending"),
      (1, "1/2/2011", "tim", "pending"),
      (2, "1/2/2011", "bob", "pending"),
      (3, "2/2/2011", "tim", "pending")
    )
  )

  // ( order_item_order_id ,  order_item_id ,  order_item_product_id, order_item_quantity,    order_ item_product_price)
  val orderItems = sparkSession.sparkContext.parallelize(
    List(
      (0, 1, "beer", 2, 20),
      (0, 2, "nuts", 3, 10),
      (1, 1, "tacos", 1, 40),
      (2, 1, "nuts", 10, 10),
      (2, 2, "tacos", 1, 30),
      (3, 1, "nuts", 1, 10)
    )
  )

  val ordersKeyed = orders.keyBy(_._1)
  val orderItemsKeyed = orderItems.keyBy(_._1)


  val joined = ordersKeyed
    .join(orderItemsKeyed)
    .map { case (oid1, ((orderId, day, cust, status), (oid2, orderItemId, prodId, quantity, price))) =>
      Line(oid1, cust, day, quantity.toInt * price.toInt)
    }

  joined.foreach(println)

  val itemsByCustDay: RDD[((String, String), Line)] = joined.keyBy(line => (line.cust, line.day))
  val itemsByOrder: RDD[(Int, Line)] = joined.keyBy(line => line.orderId)
  val itemsByDay: RDD[(String, Line)] = joined.keyBy(line => line.day)


  // Calculate total orders per customer, per day
  //
  val keyed = orders.keyBy(tup => (tup._3, tup._2))
  val byCustDayTotals: RDD[((String, String), Int)] =
    keyed.aggregateByKey(0)({ case (accum: Int, order) => accum + 1 }, (accum1, accum2) => accum1 + accum2)
  System.out.println("byCustDayTotals:");
  byCustDayTotals.foreach(println)


  // Calculate total revenue perday
  //
  val totRevByDay: RDD[(String, Double)] =
  itemsByDay.
    aggregateByKey(0D)(
      {
        case (accum, line) => accum + line.totForItem
      },
      (accum1, accum2) => accum1 + accum2
    )

  val orderCountByDay: RDD[(String, Int)] = orders.map(x => (x._2, 1)).reduceByKey((a, b) => a + b)

  // Total and avg rev per day
  val totalOrderRevAndCountByDay: RDD[(String, (Double, Int))] = totRevByDay.join(orderCountByDay)
  totalOrderRevAndCountByDay.foreach {
    case (day, (total, count)) =>
      println(s"for day $day:  count=$count.    total=$total.  avg=${total / count}.")
  }

  val totRevByOrder = itemsByOrder.
    aggregateByKey(0)(
      { case (accum: Int, line) => accum + line.totForItem },
      (accum1, accum2) => accum1 + accum2
    )
  System.out.println("totRevByOrder:");
  totRevByOrder.foreach(println)
}

