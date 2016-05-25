package com.learning.spark

import org.apache.spark.SparkContext

/**
  * Created by niccolo on 24/05/16.
  */
object TestForEach {

  def main(args: Array[String]) {

    val sc = new SparkContext("local", "BasicAvg", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(1 to 10)

    //val ns = new NotSerializable(5)
    //val valLocal_ : Int = ns.offset

    val tranformed = input.map(i => "pippo" + i)

    println("print 1:")
    tranformed.foreach(println)

    println("print 2:")
    tranformed.collect().foreach(println)
  }

}
