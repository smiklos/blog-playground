package com.smiklos.broadcast

import org.apache.spark.util.LongAccumulator

class HeavyStuff(accu: LongAccumulator, in: String, in2: String, in3: String, in4: String) extends Serializable {

  @transient lazy val nonSerializable = new NonSerializable(accu, in, in2, in3, in4)
}

class NonSerializable(accu: LongAccumulator, in: String, in2: String, in3: String, in4: String) {

  @transient private var shouldNotBeNull: String = null

  def allLength() = {
    if (shouldNotBeNull == null) {
      shouldNotBeNull = ""
      accu.add(1)
    }
    in.length + in2.length + in3.length + in4.length
  }
}
