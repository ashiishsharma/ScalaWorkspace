package com.blueglacier

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class SparkRDDTest extends FunSuite with BeforeAndAfter {

  private val master = "local[2]"
  private val appName = "spark-start"

  private var sparkContext: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sparkContext = new SparkContext(conf)
  }

  after {
    if (sparkContext != null) {
      sparkContext.stop()
    }
  }

  test("test Spark") {
    val inputFile = sparkContext.textFile("src/test/resources/input.txt")
    val counts = inputFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _);
    System.out.println(counts.toDebugString)
    counts.cache()
    //    counts.saveAsTextFile("output")
    System.out.println("OK")
  }

  test(testName = "test BroadCast variable") {
    val broadCastVar = sparkContext.broadcast(Array(1, 2, 3))
  }

  test(testName = "Add accumulator values") {
    val accum = sparkContext.accumulator(0)
    sparkContext.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x)
    System.out.println("***************************")
    System.out.println(accum.value)
    System.out.println("###########################")
  }

}
