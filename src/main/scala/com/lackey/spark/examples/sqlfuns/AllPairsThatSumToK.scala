package com.lackey.spark.examples.sqlfuns
import scala.collection.mutable

// Solution is given in two parts so that we can more easily compute the complexity. The first step
// findPairsAllowingDups involves computing the list of all pairs of numbers in array that would sum to
// k, allowing dups.
//
//  Note for first step we make two passes through the array where our operations are looking up and updating
//  things in a map (constant time operations as long as the map is sparse enough so collisions are rare), and
//   adding to the 'companions' list. Each addition to companions list is constant time since it should be a 'cons'
//  operation, and as long as there are no dups in our list the number of add's to 'companions' is only one.
//  Now.. if we allow dups in our input then this complexity argument will not hold, and we'd have to further
//  optimize.  Assuming no dups, then we have complexity N for first phase
//
//  Now, the complexity of the 'eliminateDups' phase is at most order N because our input array has to have less
//  than N pairs. We make one pass through the input array
//  to do the map operation to  ensure lowest value element is first in pair. Then we make another pass through
//  all our matches and update the pairs map. We know there can't be more than N updates to this map. So
//  complexity of 'eliminateDups' phase is at most order N.
//
//   We have 2 phases, both of order N, so we can state that the whole algorithm is order N (assuming no dups).
//

object AllPairsThatSumToK extends App {

  import scala.collection.mutable.ArrayBuffer

  def findPairsAllowingDups(k: Int, array: Seq[Int]): Seq[(Int,Int)] = {

    val pairs = ArrayBuffer[(Int,Int)]()
    val matches = mutable.Map[Int,ArrayBuffer[Int]]()

    // First pass through array computes match map. Given the difference between 'k' and one of the numbers in 'array'
    // we compute 'diff', the difference between these two. We do this because we later want to find
    // all the array positions of other numbers in the array which, when added to 'diff' will equal 'k'
    array.indices.foreach { i =>
      val diff: Int = k - array(i)
      val indices: ArrayBuffer[Int] = matches.getOrElse(diff, new ArrayBuffer[Int]())
      indices.append(i)
      matches.update(diff, indices)
    }

    // Second pass through where for each element 'i' in array we check the map to see if  there are any 'companion'
    // numbers which when added to 'i' would give 'k
    array.indices.foreach { i =>
      // each companion 'c' in 'companions' is an index into array such that array(c) + array(i) = k
      // companions may be empty if for the array(i) no other element in the array will sum to k when added to array(i)
      val companions = matches.getOrElse(array(i), Seq[Int]())
      for (c <- companions) {
        pairs +=  ( (array(i), array(c))  )
      }
    }

    pairs.toList
  }


  def eliminateDups(array: Seq[(Int,Int)]) : Seq[(Int,Int)] = {
    val pairs = mutable.Map[Int,Int]()

    // ensure lowest value element is first in pair
    val eachPairLowToHigh = array.map{case (i,j) => if (i>j) (j,i) else (i,j)}

    eachPairLowToHigh.foreach{ case(i,j) =>  pairs.update(i,j) }
    pairs.toList
  }

  def findPairs(k: Int, array: Seq[Int]): Seq[(Int,Int)] = eliminateDups(findPairsAllowingDups(k, array))

  assert( findPairs(2, Seq(0, 2, -1)) == List( (0,2)) )
  assert( findPairs(2, Seq(4, 0, 2, -2, -1)) == List((-2,4), (0,2)) )
  assert( findPairs(2, Seq[Int]()) == List[Int]())
  assert( findPairs(2, Seq(100, 500, 9)) ==  List[Int]())

  new java.text.SimpleDateFormat("yyyy-MM-dd").parse("1970-01-01").getTime / 1000

}


