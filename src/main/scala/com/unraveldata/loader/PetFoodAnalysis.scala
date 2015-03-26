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

  case class Puppy(ownerId: Long, name: String, breed: Breed, age: Int)

  case class Owner(id: Long, name: String, gender: Gender, age: Int)

  case class PetFoodPurchase(ownerId: Long, brand: String)

  def main(args: Array[String]) {


    var pw = new PrintWriter(new File("/opt/unravel/data/sample_data.txt"))


    /** initializing SparkConf **/
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("PetFoodAnalysis")
      .set("spark.executor.memory", "4g")
      .set("spark.metrics.conf","/Users/dhiraj/Documents/my_experiment/spark/conf/metrics.properties")

    val sc = new SparkContext(conf)

    /** input data source **/
    val hdfsOwnerDataInput = "/opt/unravel/data/owner_data.csv"
    val hdfsPuppyDataInput = "/opt/unravel/data/puppy_data.csv"
    val hdfsPetFoodInput = "/opt/unravel/data/pet_food_data.csv"

    val rawPuppies = sc.textFile(hdfsPuppyDataInput)
    val rawOwners = sc.textFile(hdfsOwnerDataInput)
    val rawPetFood = sc.textFile(hdfsPetFoodInput)

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

    val huskiesUnderTwoById = puppies.filter(p => (p.age < 3)).keyBy(_.ownerId)
    val huskiesUnderTwoByIdSample = huskiesUnderTwoById.takeSample(false, 10)
    pw.write("huskiesUnderTwoByIdSample :: "+huskiesUnderTwoByIdSample.toList + "\n")

    val ownersUnderTenById = owners.filter(o => o.age < 10).keyBy(_.id)
    val ownersUnderTenByIdSample = ownersUnderTenById.takeSample(false, 10)
    pw.write("ownersUnderTenByIdSample :: "+ownersUnderTenByIdSample.toList + "\n")


    val friskiesById = petFood.filter(f => f.brand == "Friskies").keyBy(_.ownerId)
    val friskiesByIdSample = friskiesById.takeSample(false, 10)
    pw.write("friskiesByIdSample :: "+friskiesByIdSample.toList + "\n")



    val cogroup: RDD[(Long, (Iterable[Puppy], Iterable[Owner], Iterable[PetFoodPurchase]))] = huskiesUnderTwoById.cogroup(ownersUnderTenById, friskiesById)
    val cogroupSample = cogroup.takeSample(false,10)
    pw.write("cogroupSample :: "+cogroupSample.toList + "\n")


    val rdd: RDD[(Option[PetFoodPurchase], Option[Owner], Int)] = cogroup.map {
      case (id, (iterHuskies, iterOwners, iterPetFood)) => {
        (iterPetFood.headOption, iterOwners.headOption, iterHuskies.map(_.age).sum + iterHuskies.size)
      }
    }

    val rddSample = rdd.takeSample(false, 2)
    pw.write("rddSample :: "+rddSample.toList + "\n")


    val filter = rdd.filter(res => (res._1 != None) && (res._2 != None))
    val filterSample = filter.takeSample(false, 10)


    val averageAgeHuskies =
      filter.map(res => (res._2, 1))
    val averageAgeHuskiesSample = averageAgeHuskies.takeSample(false, 10)
    pw.write("averageAgeHuskiesSample :: "+averageAgeHuskiesSample.toList + "\n")
    pw.close()

    val hdfsOutputPath = "/opt/unravel/data/average_husky_age"

    averageAgeHuskies.reduceByKey(_ + _).saveAsTextFile(hdfsOutputPath)
  }
}