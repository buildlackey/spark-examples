package com.lackey.spark.examples.dataset

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{Partitioner, SparkConf}

object PartitioningDataFrameViaUnderlyingRDD extends App {

  val conf =
    new SparkConf().setAppName("blah").
      setMaster("local").set("spark.sql.shuffle.partitions", "2")
  val sparkSession = SparkSession.builder .config(conf) .getOrCreate()
  val spark = sparkSession

  import spark.implicits._
  sparkSession.sparkContext.setLogLevel("ERROR")

  class CustomPartitioner(num: Int) extends Partitioner {
    def numPartitions: Int = num
    def getPartition(key: Any): Int = if (key.toString == "a") 0 else 1
  }

  case class Emp(name: String, deptId: String)
  case class Dept(deptId: String, name: String)

  val value: RDD[Emp] = spark.sparkContext.parallelize(
    Seq(
      Emp("anne", "a"),
      Emp("dave", "d"),
      Emp("claire", "c"),
      Emp("roy", "r"),
      Emp("bob", "b"),
      Emp("zelda", "z"),
      Emp("moe", "m")
    )
  )
  val employee: Dataset[Emp] = value.toDS()
  val department: Dataset[Dept] = spark.sparkContext.parallelize(
    Seq(
      Dept("a", "ant dept"),
      Dept("d", "duck dept"),
      Dept("c", "cat dept"),
      Dept("r", "rabbit dept"),
      Dept("b", "badger dept"),
      Dept("z", "zebra dept"),
      Dept("m", "mouse dept")
    )
  ).toDS()


  val dumbPartitioner: Partitioner = new CustomPartitioner(2)

  // Convert to-be-joined dataframes to custom repartition RDDs [ custom partitioner:  cp ]
  //
  val deptPairRdd: RDD[(String, Dept)] = department.rdd.map { dept => (dept.deptId, dept) }
  val empPairRdd: RDD[(String, Emp)] = employee.rdd.map { emp: Emp => (emp.deptId, emp) }

  val cpEmpRdd: RDD[(String, Emp)] = empPairRdd.partitionBy(dumbPartitioner)
  val cpDeptRdd: RDD[(String, Dept)] = deptPairRdd.partitionBy(dumbPartitioner)

  assert(cpEmpRdd.partitioner.get == dumbPartitioner)
  assert(cpDeptRdd.partitioner.get == dumbPartitioner)

  // Here we join using RDDs and ensure that the resultant rdd is partitioned so most things end up in partition 1
  val joined: RDD[(String, (Emp, Dept))] = cpEmpRdd.join(cpDeptRdd)
  val result: Array[(Array[(String, (Emp, Dept))], Int)] = joined.glom().collect().zipWithIndex
  result.foreach{
    item: Tuple2[Array[(String, (Emp, Dept))], Int] =>
      println(s"partition: ${item._2}. contents: ${item._1.toList}")
  }

  System.out.println("partitioner of RDD created by joining 2 RDD's w/ custom partitioner: " + joined.partitioner)
  assert(joined.partitioner.contains(dumbPartitioner))

  val joinedDF = joined.toDF().as[(String, (Emp, Dept))].map {
    case (id: String, (emp: Emp, dept: Dept)) =>
      (emp.name, emp.deptId, dept.name)
  }.toDF("empName", "deptId", "deptName")
  joinedDF.show()
}
