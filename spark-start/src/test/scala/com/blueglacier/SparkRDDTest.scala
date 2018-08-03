package com.blueglacier

import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfter, FunSuite}

class SparkRDDTest extends FunSuite with BeforeAndAfter {

  test("test Spark") {
    val inputfile = SparkContext.getOrCreate().textFile("input.txt")
  }

}
