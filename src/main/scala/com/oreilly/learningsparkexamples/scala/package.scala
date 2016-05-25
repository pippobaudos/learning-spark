package com.oreilly.learningsparkexamples

/**
  * Created by niccolo on 23/05/16.
  */
package object scala {


  class NotSerializableFunction extends (Int => Int) {
    override def apply(v1: Int): Int = v1 * 2
  }

}
