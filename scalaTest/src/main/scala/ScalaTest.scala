package org.klm

import scala.collection.immutable._
object ScalaTest extends App {
  var visited: HashMap[String, String] = new HashMap()
  println("enter # of test cases req")
  val n = scala.io.StdIn.readInt()
  val inputStrings = new Array[String](n)
  val outputStrings = new Array[String](n)
  for (i <- 0 to n - 1) {
    println("enter " + (i + 1) + "th String: ")
    inputStrings(i) = scala.io.StdIn.readLine()
  }

  for (i <- 0 to n - 1) {
    outputStrings(i) = reduce(inputStrings(i))
  }
  println("final results")
  outputStrings.foreach(println)


  var i = 0
  //method to reduce the original string and to identify the smallest original string
  def reduce(str: String) : String = {
    var length = str.length
    //list to store the suffixes
    var list : Set[String] = new HashSet()
    list = list + str
    //looping the suffixes from right to left
    for( i <- 1 to (str.length-i) if i <= (str.length-i) ) {
      var start = length -i
      //first - suffix
      var first = str.substring(start-i , start)
      //second - prefix
      var second = str.substring(start , length)
      //to check if the reduced string is already visited
      if(first.equals(second) && !visited.contains(str.substring(0, length - i))) {
        var result = reduce(str.substring(0, length - i))
        visited = visited+(str.substring(0, length - i) -> result)
        list = list + result
      }

    }
    //return the smallest reduced string
    list.min
  }
}