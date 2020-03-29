package com.smiklos.perf

import com.smiklos.SparkProvider
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.scalameter.picklers.noPickler._
import org.scalameter.{Bench, Gen}

object DataFramePerformanceTest extends Bench.LocalTime with SparkProvider {

  import spark.implicits._

  val base = spark.sparkContext
    .parallelize(0 to 10000000)
    .toDF("num").cache()

  val udfDF = base
    .withColumn("num", udf((in: Int) => {
      in + 1
    }).apply('num))

  val genDF = Gen.single("test")(udfDF)

  performance of "spark" in {
    measure method "just count" in {
      using(genDF) in { df =>
        df.count()
      }
    }
    measure method "filter and count" in {
      using(genDF) in { df =>
        df.filter("num < 0").count()
      }
    }
    measure method "filter all and foreach" in {
      using(genDF) in { df =>
        df.filter("num < 0").foreachPartition((it: Iterator[Row]) =>  {
          it.length
          ()
        })
      }
    }
  }
}
