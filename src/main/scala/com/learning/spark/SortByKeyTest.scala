package com.learning.spark

import org.apache.hadoop.io.LongWritable
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by niccolo on 24/05/16.
  */
object SortByKeyTest {

  case class Person(name: String, lastname: String)

  case class PersonAndFloat(p: Person, f: Float)

  case class NameWithPersonAndFloat(name: String, pf: PersonAndFloat)


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[4]")
    conf.set("spark.serializer","org,apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(
      Array(
        classOf[LongWritable],
        classOf[Array[LongWritable]],

        classOf[Person],
        classOf[Array[Person]],
        classOf[PersonAndFloat],
        classOf[NameWithPersonAndFloat],
        classOf[Array[NameWithPersonAndFloat]],

        classOf[scala.collection.mutable.WrappedArray.ofRef[_]]
      )
    )
    conf.set("spark.kryo.registrationRequired", "true")

    val sc = new SparkContext(conf)

    //val b = sc.parallelize(List(3,1,9,12,4),2)
    //val a = sc.parallelize(List("wyp", "iteblog", "com", "397090770", "test"), 2)
    //val longsRdd = b.zip(a)
    val longsRdd = sc.parallelize(Seq(new LongWritable(5)))
    longsRdd.foreach(println)
    println("longsRdd count: " + longsRdd.count())

    val peopleRdd = sc.parallelize(Seq(new Person("mario", "rossi"), new Person("luca", "delucia"), new Person("fabio", "pinolo")))
    val nameToPeopleRdd = peopleRdd.map(p => new NameWithPersonAndFloat(p.name, new PersonAndFloat(p, 1.5f))).collect()
    nameToPeopleRdd.foreach(println)
  }
}
