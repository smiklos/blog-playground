package com.smiklos

import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, ExpressionSet, If, IsNotNull, IsNull, Literal, PredicateHelper, ScalaUDF}
import org.apache.spark.sql.catalyst.optimizer.CombineFilters.splitConjunctivePredicates
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession}
import org.scalameter.picklers.IntPickler
import org.scalameter.{Bench, Gen, Measurer}
import org.scalameter.picklers.noPickler._
object UDFTest extends Bench.LocalTime {

  val spark = SparkSession
    .builder()
    .config("spark.debug.maxToStringFields", "200")
    .config("spark.ui.enabled", "false")
    .master("local[*]")
    .withExtensions(f => {
      f.injectOptimizerRule(_ => FilterUDFInputsNonNull)
    })
    .appName("loader")
    .getOrCreate()
  import spark.implicits._

  val baseDF = spark.sparkContext
    .parallelize(0 to 10000000)
    .toDF("num")
    .cache()

  val stringParquetDF = spark.read.parquet("data/udf/test-data.parquet").cache()
  val intParquetDF = spark.read.parquet("data/udf/test-data-num.parquet")

  val udfDF = baseDF
    .withColumn("num", udf((in: Int) => in + 1).apply('num))

  val udfIntegerDF = baseDF
    .withColumn("num", udf((in: Integer) => in + 1).apply('num))

  val expressionDF = baseDF
    .withColumn("num", 'num.plus(1))

  val stringBase = stringParquetDF
    .withColumn("str", substring('str, 3, 1))

  val strUDF = udf((in: String) => in.substring(0, 2))
  val strUDFNonNull = udf((in: String) => in.substring(0, 2)).asNonNullable()

  val stringBaseUDF = stringParquetDF
    .withColumn("str", strUDF.apply('str))

  //  udfDF.queryExecution.debug.codegen()
  //  udfIntegerDF.queryExecution.debug.codegen()
  //  expressionDF.queryExecution.debug.codegen()
  //  stringBase.queryExecution.debug.codegen()
  //  stringBaseUDF.queryExecution.debug.codegen()
  //  stringParquetDF.withColumn("str", strUDFNonNull.apply('str)).queryExecution.debug.codegen()
  //stringParquetDF.filter(substring('str, 0, 2).equalTo("test")).explain()
  //stringParquetDF.filter('str.isNotNull and udf((in: String) => in.substring(0, 2)).apply('str).equalTo("test")).explain(true)
  // stringParquetDF.filter(udf((in: String) => in.substring(0, 2)).asNonNullable().apply('str).equalTo("test")).explain(true)
  // intParquetDF.filter(udf((in: Int) =>  in).apply(col("num")).equalTo(5)).explain(true)
  val nullableUDFDF = Gen.single("nullableUDFDF")(stringParquetDF
    .withColumn("sub_str", udf((in: String) => in.substring(0, 2)).apply('str))
    .withColumn("first_char", substring($"sub_str", 0, 1))
    .withColumn("last_char", substring($"sub_str", -1, 1)))

  val nonNullableUDFDF = Gen.single("nonNullableUDFDF")(stringParquetDF
    .withColumn("sub_str", udf((in: String) => in.substring(0, 2)).asNonNullable().apply('str))
    .withColumn("first_char", substring($"sub_str", 0, 1))
    .withColumn("last_char", substring($"sub_str", -1, 1)))

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
  } yield stringParquetDF

  val udfGenString = stringParquetGen.map(_.withColumn("str", strUDF.apply(strUDF.apply(strUDF.apply(strUDF.apply(strUDF.apply('str)))))))
  val udfGenStringNonNullable = stringParquetGen.map(_.withColumn("str", strUDFNonNull.apply(strUDFNonNull.apply(strUDFNonNull.apply(strUDFNonNull.apply(strUDFNonNull.apply('str)))))))
  val expGenString = stringParquetGen.map(_.withColumn("str", substring(substring(substring(substring(substring('str, 3, 1), 5, 3), 2, 5), 4, 32), 0, 12)))

  performance of "functions" in {
    //    measure method "udf" in {
    //      using(udfGen) in { df =>
    //        df.filter("num < 0").foreachPartition((it: Iterator[Row]) =>  {
    //          it.length
    //          ()
    //        })
    //      }
    //    }
    //    measure method "udfInteger" in {
    //      using(udfGenInteger) in { df =>
    //        df.filter("num < 0").foreachPartition((it: Iterator[Row]) =>  {
    //          it.length
    //          ()
    //        })
    //      }
    //    }
    //    measure method "exp" in {
    //      using(expGen) in { df =>
    //        df.filter("num < 0").foreachPartition((it: Iterator[Row]) =>  {
    //          it.length
    //          ()
    //        })
    //      }
    //    }
    //
    //    measure method "udfString" in {
    //      using(udfGenString) in { df =>
    //        df.filter("str is not null").foreachPartition((it: Iterator[Row]) =>  {
    //          it.length
    //          ()
    //        })
    //      }
    //    }
    //    measure method "udfString NonNull" in {
    //      using(udfGenStringNonNullable) in { df =>
    //        df.filter("str is not null").foreachPartition((it: Iterator[Row]) =>  {
    //          it.length
    //          ()
    //        })
    //      }
    //    }
    //    measure method "expString" in {
    //      using(expGenString) in { df =>
    //        df.filter("str is not null").foreachPartition((it: Iterator[Row]) =>  {
    //          it.length
    //          ()
    //        })
    //      }
    //    }
    measure method "nullableUDFDF" in {
      using(nullableUDFDF) in { df =>
        df.filter("first_char = 'x' and last_char = 'x'").foreachPartition((it: Iterator[Row]) => {
          it.length
          ()
        })
      }
    }

    measure method "nonNullableUDFDF" in {
      using(nonNullableUDFDF) in { df =>
        df.filter("first_char = 'x' and last_char = 'x'").foreachPartition((it: Iterator[Row]) => {
          it.length
          ()
        })
      }
    }

  }

}

object FilterUDFInputsNonNull extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Filter(fc, child) =>
      Filter(splitConjunctivePredicates(fc).map {
        case e @ EqualTo(If(IsNull(_), _, s: ScalaUDF), l @ Literal(litVal, dataType)) =>
          // if none of the inputs handle null, all the inputs are of primitive types
          val safeToPushdown = s.inputsNullSafe.forall(i => i)
          val udfCallWithoutIsNull = EqualTo(s, l)
          if (safeToPushdown) {
            s.children.map(IsNotNull).reduceOption(And)
              .map(filters => And(filters, udfCallWithoutIsNull))
              .getOrElse(udfCallWithoutIsNull)
          } else e
        case other => other
      }.reduce(And), child)
  }
}