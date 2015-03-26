package com.unraveldata.loader

/**
 * Created by dhiraj on 3/25/15.
 */

import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object PetFoodAnalysis {

  /** faking type classes **/
  type Breed = String
  type Gender = String
  val enableSample: Boolean = true

  case class Puppy(ownerId: Long, name: String, breed: Breed, age: Int)

  case class Owner(id: Long, name: String, gender: Gender, age: Int)

  case class PetFoodPurchase(ownerId: Long, brand: String)

  def main(args: Array[String]) {
    performTask()
    performTask()
  }

  def performTask() {

    println("Starting performTask :::")
    val time = scala.compat.Platform.currentTime

    var pw = new PrintWriter(new File("/opt/unravel/data/sample_data.txt"))


    /** initializing SparkConf **/
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("PetFoodAnalysis")
      .set("spark.executor.memory", "4g")
      .set("spark.metrics.conf", "/Users/dhiraj/Documents/my_experiment/spark/conf/metrics.properties")

    val sc = new SparkContext(conf)

    /** input data source **/
    val hdfsOwnerDataInput = "/opt/unravel/data/owner_data.csv"
    val hdfsPuppyDataInput = "/opt/unravel/data/puppy_data.csv"
    val hdfsPetFoodInput = "/opt/unravel/data/pet_food_data.csv"

    val rawPuppies = sc.textFile(hdfsPuppyDataInput)
    if (enableSample) {
      val rawPuppiesSample = rawPuppies.takeSample(false, 10)
      pw.write("rawPuppiesSample :: " + rawPuppiesSample.toList + "\n\n\n\n")
    }
    val rawOwners = sc.textFile(hdfsOwnerDataInput)
    if (enableSample) {
      val rawOwnersSample = rawOwners.takeSample(false, 10)
      pw.write("rawOwnersSample :: " + rawOwnersSample.toList + "\n\n\n\n")
    }
    val rawPetFood = sc.textFile(hdfsPetFoodInput)
    if (enableSample) {
      val rawPetFoodSample = rawPetFood.takeSample(false, 10)
      pw.write("rawPetFoodSample :: " + rawPetFoodSample.toList + "\n\n\n\n")
    }
    val puppies = rawPuppies.map { pup =>
      pup.split(",") match {
        case Array(id, n, t, a) => Puppy(id.toLong, n, t, a.toInt)
        case _ => throw new Exception("Failed to parse Puppy")
      }
    }

    val owners = rawOwners.map { owner =>
      owner.split(",") match {
        case Array(id, n, g, a) => Owner(id.toLong, n, g, a.toInt)
        case _ => throw new Exception("Failed to parse Owner")
      }
    }

    val petFood = rawPetFood.map { pf =>
      pf.split(",") match {
        case Array(id, b) => PetFoodPurchase(id.toLong, b)
        case _ => throw new Exception("Failed to parse PetFoodPurchase")
      }
    }

    val huskiesUnderTwoByIdfilter1: RDD[Puppy] = puppies.filter(p => (p.age < 2))
    val huskiesUnderTwoById = huskiesUnderTwoByIdfilter1.keyBy(_.ownerId)

    if (enableSample) {
      val huskiesUnderTwoByIdSample = huskiesUnderTwoById.takeSample(false, 10)
      pw.write("huskiesUnderTwoByIdSample :: " + huskiesUnderTwoByIdSample.toList + "\n\n\n\n")
    }

    val ownersUnderTenByIdfilter1: RDD[Owner] = owners.filter(o => o.age < 10)
    val ownersUnderTenById = ownersUnderTenByIdfilter1.keyBy(_.id)

    if (enableSample) {
      val ownersUnderTenByIdSample = ownersUnderTenById.takeSample(false, 10)
      pw.write("ownersUnderTenByIdSample :: " + ownersUnderTenByIdSample.toList + "\n\n\n\n")
    }

    val friskiesByIdfilter1: RDD[PetFoodPurchase] = petFood.filter(f => f.brand == "Friskies")
    val friskiesById = friskiesByIdfilter1.keyBy(_.ownerId)

    if (enableSample) {
      val friskiesByIdSample = friskiesById.takeSample(false, 10)
      pw.write("friskiesByIdSample :: " + friskiesByIdSample.toList + "\n\n\n\n")
    }


    val cogroup: RDD[(Long, (Iterable[Puppy], Iterable[Owner], Iterable[PetFoodPurchase]))] = huskiesUnderTwoById.cogroup(ownersUnderTenById, friskiesById)

    if (enableSample) {
      val cogroupSample = cogroup.takeSample(false, 10)
      pw.write("cogroupSample :: " + cogroupSample.toList + "\n\n\n\n")
    }

    val rdd: RDD[(Option[PetFoodPurchase], Option[Owner], Int)] = cogroup.map {
      case (id, (iterHuskies, iterOwners, iterPetFood)) => {
        (iterPetFood.headOption, iterOwners.headOption, if (iterHuskies.size == 0) 0 else iterHuskies.map(_.age).sum / iterHuskies.size)
      }
    }

    if (enableSample) {
      val rddSample = rdd.takeSample(false, 2)
      pw.write("rddSample :: " + rddSample.toList + "\n\n\n\n")
    }

    val filter = rdd.filter(res => (res._1 != None) && (res._2 != None))
    if (enableSample) {
      val filterSample = filter.takeSample(false, 10)
      pw.write("filterSample :: " + filterSample.toList + "\n\n\n\n")
    }


    val averageAgeHuskies =
      filter.map(res => (res._2, 1))


    if (enableSample) {
      val averageAgeHuskiesSample = averageAgeHuskies.takeSample(false, 10)
      pw.write("averageAgeHuskiesSample :: " + averageAgeHuskiesSample.toList + "\n\n\n\n")
    }

    pw.close()

    val hdfsOutputPath = "/opt/unravel/data/average_husky_age"

    averageAgeHuskies.reduceByKey(_ + _).saveAsTextFile(hdfsOutputPath)

    val endTime = scala.compat.Platform.currentTime

    println("total time : " + (endTime - time))


  }
}