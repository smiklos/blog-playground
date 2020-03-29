package com.smiklos.broadcast

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.scalameter.api._
import org.scalameter.picklers.noPickler._
class BroadCastTest(helper: HeavyStuff, @transient spark: SparkSession, accu: LongAccumulator)
  extends Serializable
    with LazyLogging {

  val broadCastAccu = spark.sparkContext.longAccumulator("broadcast-accut")
  val cfgBroadcasted = spark.sparkContext.broadcast(
    new HeavyStuff(broadCastAccu, "test", "lofodsfosdfosdfosdfo", "sdfsfdsfosdfosdfsodfsodf", "asdasdmasdmsadmdsasd")
    )

  val initial = spark.sparkContext.parallelize(0 to 500, 50).cache()

  val broadCastGen = initial.map(_ => cfgBroadcasted.value.nonSerializable.allLength())

  val repeatedGen = initial
    .map(_ => {
      helper.nonSerializable.allLength()
    })
    .sum()

  println("########################## NEW JOB ###################")
  broadCastGen.sum()
  logger.info(accu.value + " was the reuse accue")
  logger.info(broadCastAccu.value + " was the broadcast accu")

}

object BroadCastTest extends App {

  val spark = SparkSession
    .builder()
    .config("spark.debug.maxToStringFields", "200")
    .config("spark.ui.enabled", "false")
    .appName("loader")
    .getOrCreate()

  val accu = spark.sparkContext.longAccumulator("re-invocation")
  val heavy = new HeavyStuff(accu, "test", "lofodsfosdfosdfosdfo", "sdfsfdsfosdfosdfsodfsodf", "asdasdmasdmsadmdsasd")

  new BroadCastTest(heavy, spark, accu)
}
