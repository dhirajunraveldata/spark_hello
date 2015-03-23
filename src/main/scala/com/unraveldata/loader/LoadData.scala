package com.unraveldata.loader

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dhiraj on 3/5/15.
 */
object LoadData {

  def main(args: Array[String]) {

    val data = 1 to 10000

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("CountingSheep")
      .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)


    val distData = sc.parallelize(data)
    val count = distData.filter(a => a > 50).count()
    println(" count =  " + count)


  }

}
