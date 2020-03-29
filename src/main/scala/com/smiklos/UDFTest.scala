package com.smiklos

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.scalameter.picklers.IntPickler
import org.scalameter.{Bench, Gen, Measurer}
import org.scalameter.picklers.noPickler._
object UDFTest extends Bench.LocalTime {

  val spark = SparkSession
    .builder()
    .config("spark.debug.maxToStringFields", "200")
    .config("spark.ui.enabled", "false")
    .master("local[*]")
    .appName("loader")
    .getOrCreate()
  import spark.implicits._

  val baseDF = spark.sparkContext
    .parallelize(0 to 10000000)
    .toDF("num")
    .cache()

  val stringParquetDF = spark.read.parquet("data/udf/test-data.parquet").cache()

  val udfDF = baseDF
    .withColumn("num", udf((in: Int) => in + 1).apply('num))

  val udfIntegerDF = baseDF
    .withColumn("num", udf((in: Integer) => in + 1).apply('num))

  val expressionDF = baseDF
    .withColumn("num", 'num.plus(1))

  val stringBase = stringParquetDF
    .withColumn("str2", substring('str, 0, 2))

  val stringBaseUDF = stringParquetDF
    .withColumn("str2", udf((in: String) => in.substring(0, 2)).apply('str))

  udfDF.queryExecution.debug.codegen()
  udfIntegerDF.queryExecution.debug.codegen()
  expressionDF.queryExecution.debug.codegen()
  stringBase.queryExecution.debug.codegen()
  stringBaseUDF.queryExecution.debug.codegen()


  val numberGen = Gen.single("test")(baseDF)


  val udfGen = numberGen.map(_.withColumn("num", udf((in: Int) => {
    in + 1
  }).apply('num)))

  val udfGenInteger = numberGen.map(_.withColumn("num", udf((in: Integer) => {
    in + 1
  }).apply('num)))

  val expGen = numberGen.map(_.withColumn("num", 'num.plus(1)))

  val stringParquetGen = for {
    size <- Gen.single("test")(10000000)(IntPickler)
  } yield
    stringParquetDF


  val udfGenString = stringParquetGen.map(_.withColumn("str2", udf((in: String) => in.substring(0, 2)).apply('str)))
  val expGenString = stringParquetGen.map(_.withColumn("str2", substring('str, 0, 2)))

  performance of "functions" in {
    measure method "udf" in {
      using(udfGen) in { df =>
        df.filter("num < 0").foreachPartition((it: Iterator[Row]) =>  {
          it.length
          ()
        })
      }
    }
    measure method "udfInteger" in {
      using(udfGenInteger) in { df =>
        df.filter("num < 0").foreachPartition((it: Iterator[Row]) =>  {
          it.length
          ()
        })
      }
    }
    measure method "exp" in {
      using(expGen) in { df =>
        df.filter("num < 0").foreachPartition((it: Iterator[Row]) =>  {
          it.length
          ()
        })
      }
    }

    measure method "udfString" in {
      using(udfGenString) in { df =>
        df.filter("str2 is not null").foreachPartition((it: Iterator[Row]) =>  {
          it.length
          ()
        })
      }
    }
    measure method "expString" in {
      using(expGenString) in { df =>
        df.filter("str2 is not null").foreachPartition((it: Iterator[Row]) =>  {
          it.length
          ()
        })
      }
    }

  }
}
