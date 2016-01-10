package com.dudanov.util.dynamicconnectivity

import org.scalatest.{FunSuite, Matchers}


class DynamicConnectivityTest extends FunSuite with SharedSparkContext with Matchers {

  test("Run Dynamic Connectivity With Test Data Set") {
    val data = sc.textFile(getClass.getClassLoader.getResource("testdata.txt").getPath)
      .map(x => x.split(",")).map(x => Array(x(0).toLong, x(1).toLong))

    val result = DynamicConnectivity(data)

    result.foreach(println)
  }

  test("Record 4 and 8 Should be Connected") {
    val data = sc.textFile(getClass.getClassLoader.getResource("testdata.txt").getPath)
      .map(x => x.split(",")).map(x => Array(x(0).toLong, x(1).toLong))

    val result = DynamicConnectivity(data)

    assert(result.filter(x => x._2 == 4).collect()(0)._1 === result.filter(x => x._2 == 8).collect()(0)._1)
  }

  test("Record 1 and 9 Should NOT be Connected") {
    val data = sc.textFile(getClass.getClassLoader.getResource("testdata.txt").getPath)
      .map(x => x.split(",")).map(x => Array(x(0).toLong, x(1).toLong))

    val result = DynamicConnectivity(data)

    assert(result.filter(x => x._2 == 1).collect()(0)._1 != result.filter(x => x._2 == 9).collect()(0)._1)
  }

  test("The Total Number of Clusters Should be 2") {
    val data = sc.textFile(getClass.getClassLoader.getResource("testdata.txt").getPath)
      .map(x => x.split(",")).map(x => Array(x(0).toLong, x(1).toLong))

    val result = DynamicConnectivity(data)


    assert(result.map(x => x._1).distinct().count() === 2)
  }

  test("The Number of Unique Records Should be 10") {
    val data = sc.textFile(getClass.getClassLoader.getResource("testdata.txt").getPath)
      .map(x => x.split(",")).map(x => Array(x(0).toLong, x(1).toLong))

    val result = DynamicConnectivity(data)


    assert(result.map(x => x._2).count() === 10 && result.map(x => x._2).distinct().count() === 10)
  }
}
