package com.incra.datascience

import org.apache.spark.sql.SparkSession

object DataFrameExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark2S 01")
      .config("spark.master", "local")
      .getOrCreate()

    // Create a data frame
    val base = "./data/"
    val dataWithoutHeader = spark.read.
      option("inferSchema", true).
      option("header", false).
      csv(base + "peopleWithHeader.txt")

    val df = spark.read.option("header", "true").csv(base + "peopleWithHeader.txt")
    df.show()

    // This is relatively fast
    val dfs = spark.read.json(base + "employee.json")
    dfs.show()
    dfs.printSchema()

    // this is faster
    dfs.filter(dfs.col("age").gt(23)).show()
  }

}

