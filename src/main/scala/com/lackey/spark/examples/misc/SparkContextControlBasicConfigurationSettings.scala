package com.lackey.spark.examples.misc

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.io.{BufferedSource, Source}

object SparkContextControlBasicConfigurationSettings extends App {

  val conf = new SparkConf().setAppName("blah").setMaster("local").set("spark.sql.shuffle.partitions", "4")
  val sparkSession = SparkSession.builder
    .config(conf)
    .getOrCreate()
  val spark = sparkSession
  sparkSession.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val employee = spark.sparkContext.parallelize(
    List("Bob", "Alice", "Fred", "Sam", "1", "2", "3", "4", "12", "13", "14", "15")).toDF("name")
  val department = spark.sparkContext.parallelize(List(
    ("Fred", "Accounts"),
    ("1", "Accounts"),
    ("12", "Accounts"),
    ("13", "Accounts"),
    ("14", "Accounts"),
    ("15", "Accounts"),
    ("2", "Accounts"),
    ("3", "Accounts"),
    ("4", "Accounts"),
    ("Sam", "Accounts"),
    ("Bob", "Accounts"),
    ("Alice", "Sales"))).toDF("name", "department")
  val jdf = employee.join(department, "name")

  assert("4" == spark.sparkContext.getConf.get("spark.sql.shuffle.partitions"))
  assert(jdf.rdd.partitions.length.toString == spark.sparkContext.getConf.get("spark.sql.shuffle.partitions"))

  import sys.process._

  "rm -rf /tmp/outfoo1".!
  jdf.write.option("delimiter", "*").csv("/tmp/outfoo1")

  val lsOutput = "ls /tmp/outfoo1".!!
  System.out.println("lsOutput:\n" + lsOutput);

  
  new File("/tmp/outfoo1").listFiles().filter(_.getAbsolutePath.endsWith("csv")).
    foreach{ file =>
      Source.fromFile(file).getLines().foreach(println)
    }





 // read back in using format / load  (per exam objectives)
  val  jj = spark.read.format("csv").option("delimiter", "*").load("/tmp/outfoo1")
  println("dumping rows from dataframe resulting from reading back csv output")
  jj.show()

  // Verify that output directory contains 4 files of the form part-00002-d4dc6a09-.....csv
  assert(new File("/tmp/outfoo1").listFiles().count(_.getName.startsWith("part-")) == 4)
}
