package com.smiklos

import org.apache.spark.sql.SparkSession

trait SparkProvider {

  val spark = SparkSession
    .builder()
    .config("spark.debug.maxToStringFields", "500")
    .config("spark.ui.enabled", "false")
    .master("local[*]")
    .appName("sparky")
    .getOrCreate()

}
