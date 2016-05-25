package com.learning.spark

import org.apache.spark.SparkContext

/**
  * Created by niccolo on 24/05/16.
  */
object DebugUnexpectedOnPassingByValue {


  class NotSerializable(val offset: Integer) {
    def getOffset: Int = {
      return this.offset
    }
  }

  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "BasicAvg", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(1 to 10)

    val ns = new NotSerializable(5)

    val valLocal_ : Int = ns.offset

    input.map(s => s + valLocal_).foreach(println)

  }


}
