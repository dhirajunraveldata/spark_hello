package com.unraveldata.loader

import java.io.PrintWriter

/**
 * Created by dhiraj on 3/25/15.
 */
object DataGenerator {

  val owners: Int =  10000000
  val puppies: Int = 30000000
  val petFood: Int = 10000000

  val puppyNames = Array("Bella","Buddy","Max","Maggie","Bailey	","Daisy","Lucy","Chloe","Molly","Sophie")
  val humanNames = Array("James	Butt","Josephine	Darakjy","Art	Venere","Lenna	Paprocki","Donette	Foller","Simona	Morasca","Mitsue	Tollner","Leota	Dilliard","Sage	Wieser","Shivnath")

  def main(args: Array[String]) {
    import java.io._

    var pw = new PrintWriter(new File("/opt/unravel/data/owner_data.csv"))
    writeOwner(pw)
    pw.close

    pw = new PrintWriter(new File("/opt/unravel/data/puppy_data.csv"))
    writePuppy(pw)
    pw.close


    pw = new PrintWriter(new File("/opt/unravel/data/pet_food_data.csv"))
    writePetFood(pw)
    pw.close


  }


  def writeOwner(pw: PrintWriter): Unit = {
    //val a:String =
    var r = new scala.util.Random

    for (x <- 1 to owners) {
      var gen: String = if (r.nextInt(2) == 1) "M" else "F"
      pw.write(x + "," + humanNames(r.nextInt(10)) + "," + gen + "," + r.nextInt(15)+"\n")
    }
  }

  def writePuppy(pw: PrintWriter): Unit = {

    var r = new scala.util.Random
    val breeds = Array("Friskies", "boo", "foo")
    for (x <- 1 to puppies) {
      var gen: String = if (r.nextInt(2) == 1) "M" else "F"

      pw.write(r.nextInt(owners) + "," + puppyNames(r.nextInt(10)) + "," + breeds(r.nextInt(3)) + "," + r.nextInt(4)+"\n")
    }
  }


  def writePetFood(pw: PrintWriter): Unit = {

    var r = new scala.util.Random
    val brands = Array("Friskies", "beeta")
    for (x <- 1 to petFood) {
      pw.write(r.nextInt(owners) + ","  + brands(r.nextInt(2))+"\n" )
    }
  }



}
