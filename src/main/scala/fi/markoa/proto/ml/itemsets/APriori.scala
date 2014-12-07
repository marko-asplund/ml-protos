package fi.markoa.proto.ml.itemsets

import scala.annotation.tailrec
import scala.io.Source
import scala.collection.{Set, Iterator}

/*
implementation of the A-Priori algorithm.
@author markoa

TODO
- clean up
  + esp. getTriangularMatrixSize
  + getPairsIndex currying?
- generalize iterating over input data across all passes
- make dumpPairs output item names
- implement counting for set size k
- test with large data sets
 */
object APriori {
  class FrequentItems(val counts: IndexedSeq[Int], val itemMapping: Map[String, Int]) {
    def +(item: String, count: Int = 1) = if(itemMapping.contains(item))
      FrequentItems(counts.updated(itemMapping(item), counts(itemMapping(item)) + count), itemMapping)
    else
      FrequentItems(counts :+ count, itemMapping.updated(item, counts.size))
    def get = (counts, itemMapping)
  }

  object FrequentItems {
    val Initial = new FrequentItems(Vector(-1), Map())
    def apply(counts: IndexedSeq[Int], itemMapping: Map[String, Int]) = new FrequentItems(counts, itemMapping)
  }

  def createFrequentItemsTable(lines: Iterator[String], support: Int) = {
    @tailrec def countAllItems(lines: Iterator[String], basket: Int, allItems: FrequentItems): FrequentItems = {
      if (!lines.hasNext)
        allItems
      else {
        val lineItems = lines.next().split(' ').foldLeft(allItems)( (items, item) => items + item )
        countAllItems(lines, basket + 1, lineItems)
      }
    }
    @tailrec def filterFrequentItems(items: List[(String, Int)], counts: IndexedSeq[Int], freqItems: FrequentItems): FrequentItems = {
      items match {
        case List() => freqItems
        case (item, idx) :: xs if(counts(idx) > support) =>
          filterFrequentItems(xs, counts, freqItems + (item, counts(idx)))
        case (item, idx) :: xs =>
          filterFrequentItems(xs, counts, freqItems)
      }
    }
    val f = countAllItems(lines, 1, FrequentItems.Initial)
    filterFrequentItems(f.get._2.toList, f.get._1, FrequentItems.Initial)
  }

  @tailrec def countFrequentPairs(lines: Iterator[String], frequentItems: FrequentItems, counts: IndexedSeq[Int]): IndexedSeq[Int] = {
    if (!lines.hasNext)
      counts
    else {
      // iterate items in basket; filter out non-frequent items; generate pairs, order items in pair
      countFrequentPairs(lines, frequentItems, lines.next().split(' ').collect {
        case x if(frequentItems.get._2.contains(x)) => frequentItems.get._2(x) }.
        toSet.subsets(2).map( s => s.toIndexedSeq match {
        case Seq(a, b) => if(a < b) (a, b) else (b, a)
      }).foldLeft(counts) { (countsAcc, p) =>
        val k = getPairsIndex(p._1, p._2, frequentItems.get._2.size)
        countsAcc.updated(k, counts(k) + 1)
      })
    }
  }

  def getPairsIndex(i: Int, j: Int, n: Int) = ((i-1) * (n-i/2.0)).toInt + j - i

  def getTriangularMatrixSize(n: Int) = ((n-1) * n/2.0).toInt + 1

  def getFrequentPairCounts(support: Int, n: Int, items: FrequentItems, counts: IndexedSeq[Int]) = {
    val idxToName = for((k, v) <- items.get._2) yield (v, k)
    (for(i <- 1 to (n-1);
      j <- 1 to n if(i < j);
      k = getPairsIndex(i, j, n) if(counts(k) > support))
      yield (idxToName(i), idxToName(j)) -> counts(k)).toMap
  }

  def countFrequentPairs(fileName: String, support: Int): (FrequentItems, IndexedSeq[Int]) = {
    val lines = Source.fromFile(fileName).getLines()
    lines.next()
    val items = createFrequentItemsTable(lines, support)

    val lines2 = Source.fromFile(fileName).getLines()
    lines2.next()
    val pairCounts = countFrequentPairs(lines2, items, new Array[Int](getTriangularMatrixSize(items.get._2.size)))
    (items, pairCounts)
  }

  def main(args: Array[String]): Unit = {
    println(args.toList)

    val r = countFrequentPairs(args(0), 2)
    getFrequentPairCounts(2, (r._1.get._2.size-1)/2, r._1, r._2)

    // TODO: set size k
  }

}
