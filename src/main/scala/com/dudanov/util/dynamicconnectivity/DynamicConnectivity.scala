/**
  * The <tt>DynamicConnectivity</tt> class represents a
  * Spark job that implements disjoint-set data structure.
  * It supports the <em>union</em> and <em>find</em> operations, along with
  * methods for determining whether two records are in the same cluster
  * and the total number of clusters.
  * <p>
  * This implementation uses weighted dynamic connectivity (by cluster size) with full path compression.
  * Initializing a data structure with <em>N</em> unique elements (records) takes some time.
  * However, <em>union</em>, <em>find</em>, and <em>connected</em> take
  * logarithmic time (in the worst case) and <em>numberOfClusters</em> takes constant
  * time. Moreover, the amortized time per <em>union</em>, <em>find</em>,
  * and <em>connected</em> operation has inverse Ackermann complexity.
  * <p>
  * For additional documentation, see <a href="https://en.wikipedia.org/wiki/Dynamic_connectivity">Dynamic Connectivity</a>,
  * <a href="https://en.wikipedia.org/wiki/Disjoint-set_data_structure">Disjoint-set Data Structure</a> and
  * <a href="http://algs4.cs.princeton.edu/15uf/index.php#1.5">Case Study: Union-Find by Robert Sedgewick</a>.
  *
  * @author Ivan Dudanov
  */

package com.dudanov.util.dynamicconnectivity

import org.apache.spark.rdd.RDD

import scala.math._
import java.util.UUID

trait DynamicConnectivity {
  def apply(f: => RDD[Array[Long]]): RDD[(Long, Long)]
}

/**
  * Let me describe what we are trying to solve here :)
  * Please imagine an algorithm that matches some records. It could be anything; customers, error logs... For example,
  * we have got two random strings, and we are trying to find out how similar they are. Lets say we are using
  * Levenshtein distance, and we are converting distance to similarity. Also, lets assume that each record has it's own
  * unique identifier that could be any type by the way.
  *
  * So, we are matching records, in order to do that we need to pair up every record with every other record in our
  * data set. We will end up with a lot of pairs (combinations) with Levenshtein distances (similarities) associated
  * with pairs.
  *
  * Now, we are filtering based on similarity, we do not need anything that less then lets say 0.8.
  *
  * Here are our pairs, identifier 0 comma identifier 1. We excluded similarities, since we know that they all greater
  * or equal to 0.8 so they definitely linked.
  *
  * 4,3
  * 3,8
  * 6,5
  * 9,4
  * 2,1
  * 8,9
  * 5,0
  * 7,2
  * 6,1
  * 1,0
  * 6,7
  *
  * OK, that was just some background story :) now the real problem - how to form disjoint-sets (clusters) from this
  * data, and also assign an identifier to them? Here is en Example: 4 - 3 - 8 - 9 this is one, 1 - 0 - 6 - 7 - 2 - 5
  * this is two.
  *
  * 6701996089449296089,4
  * 4854454773834560458,1
  * 4854454773834560458,0
  * 6701996089449296089,3
  * 4854454773834560458,6
  * 4854454773834560458,7
  * 6701996089449296089,8
  * 6701996089449296089,9
  * 4854454773834560458,2
  * 4854454773834560458,5
  *
  * Actually that was a real business requirement. In my case it was a real customer data and they wanted to use a
  * single point of contact instead of sending one completely identical email to six different email addresses :)
  *
  * I re-implemented this algorithm over the weekend. On production, I ran similar code on Spark on millions of records.
  *
  */

object DynamicConnectivity {

  def apply(data: => RDD[Array[Long]]): RDD[(Long, Long)] = {

/** This implementation is backed up by an integer array data structure, so we need to translate existing identifiers
  * which could be any data type (strings or longs for example) to integers. These integers will be valid just for one
  * particular run. We going to use them to form a union-find data structure.
  */
    val translator = data.flatMap(x => x).distinct.zipWithIndex
    val translatorLocal = translator.collect.toMap

    val pairs = data.map(x => Pair(translatorLocal(x(0)).toInt, translatorLocal(x(1)).toInt))

    n = translatorLocal.size
    count = n
    parent = Array.range(0, n)
    size = Array.fill(n)(1)

    val indexedPairs = pairs.zipWithIndex

    val step = (5 * pow(10, 5)).toInt
    val hi = step.toInt
    val lo = 0

    chunkByChunk(hi, lo, step)

    /**
      * Reads in large chunks (500000) of pairs of integers (between 0 and N-1) from RDD,
      * where each integer represents some object/identifier.
      */

    def chunkByChunk(hi: Int, lo: Int, step: Int): Unit = {
      val pairsLocal = indexedPairs.filter(x => x._2 >= lo && x._2 < hi).map(x => (x._1.p, x._1.q)).collect
      for (pair <- pairsLocal) {
        val p = pair._1
        val q = pair._2

        union(p, q)
      }
      val newLo = lo + pairsLocal.length
      val newHi = newLo + step
      if (pairsLocal.length == step) {
        chunkByChunk(newHi, newLo, step)
      }
    }

    val s0 = parent.toSet
    val s1 = s0.map(x => find(x)).toSet
    val cluster = s1.map(x => (x, abs(UUID.randomUUID().getMostSignificantBits()))).toMap

    translator.map(x => (cluster(find(x._2.toInt)), x._1))
  }

  var n = 0
  var count = n
  var parent = Array[Int]()
  var size = Array[Int]()

  /**
    * Returns the cluster identifier for the cluster containing record <tt>p</tt>.
    *
    * @param  p the integer representing one object
    * @return the cluster identifier for the cluster containing record <tt>p</tt>
    */

  def find(p: Int): Int = {
    var root = p
    while (root != parent(root)) {
      root = parent(root)
    }
    // Full Path Compression. Making all the nodes that we examine directly link to the root
    var tempP = p
    while (tempP != root) {
      val newP = parent(tempP)
      parent(tempP) = root
      tempP = newP
    }
    root
  }

  /**
    * Merges the cluster containing record <tt>p</tt> with the
    * the cluster containing record <tt>q</tt>.
    *
    * @param  p the integer representing one record
    * @param  q the integer representing the other record
    */

  def union(p: Int, q: Int): Unit = {
    val rootP = find(p)
    val rootQ = find(q)
    if (rootP != rootQ) {
      // Weighted quick-union. Instead of arbitrarily connecting the second cluster to the first for union(),
      // we keep track of the size of each cluster and always connect the smaller one to the larger.
      if (size(rootP) < size(rootQ)) {
        parent(rootP) = rootQ
        size(rootQ) += size(rootP)
      } else {
        parent(rootQ) = rootP
        size(rootP) += size(rootQ)
      }
      count = count - 1
    }
  }

  /**
    * Returns true if the two records are in the same cluster.
    *
    * @param  p the integer representing one record
    * @param  q the integer representing the other record
    * @return <tt>true</tt> if the two records <tt>p</tt> and <tt>q</tt> are in the same cluster;
    *         <tt>false</tt> otherwise
    */

  def connected(p: Int, q: Int): Boolean = {
    find(p) == find(q)
  }

  def numberOfClusters(): Int = {
    count
  }

  case class Pair(p: Int, q: Int)

}



