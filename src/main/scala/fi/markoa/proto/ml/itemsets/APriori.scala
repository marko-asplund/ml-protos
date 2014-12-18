package fi.markoa.proto.ml.itemsets

import scala.annotation.tailrec
import scala.io.Source
import scala.collection.{Set, Iterator}

/*
implementation of the A-Priori algorithm.
@author markoa

TODO
- clean up
  + simplification, readability
  + esp. getTriangularMatrixSize
  + getPairsIndex currying?
- generalize iterating over input data across all passes
- implement counting for set size k
- test with large data sets
 */
object APriori {
  class FrequentItems(val counts: IndexedSeq[Int], val itemMapping: Map[String, Int]) {
    def +(item: String, count: Int = 1) = if(itemMapping.contains(item))
      FrequentItems(counts.updated(itemMapping(item), counts(itemMapping(item)) + count), itemMapping)
    else
      FrequentItems(counts :+ count, itemMapping.updated(item, counts.size))
  }

  object FrequentItems {
    val Initial = new FrequentItems(Vector(-1), Map())
    def apply(counts: IndexedSeq[Int], itemMapping: Map[String, Int]) = new FrequentItems(counts, itemMapping)
  }

  @tailrec def createFrequentItemsTable(lines: Iterator[String], support: Int, items: FrequentItems = FrequentItems.Initial): FrequentItems = {
    if(!lines.hasNext) {
      items.itemMapping.foldLeft(FrequentItems.Initial) {
        case (itemsAcc, (item, idx)) if (items.counts(idx) > support) => itemsAcc + (item, items.counts(idx))
        case (itemsAcc, _) => itemsAcc
      }
    } else {
      createFrequentItemsTable(lines, support, lines.next().split(' ').foldLeft(items)( (itemsAcc, item) => itemsAcc + item ))
    }
  }

  @tailrec def countFrequentPairs(lines: Iterator[String], frequentItems: FrequentItems, counts: IndexedSeq[Int]): IndexedSeq[Int] = {
    if (!lines.hasNext)
      counts
    else {
      // iterate items in basket; filter out non-frequent items; generate pairs, order items in pair
      countFrequentPairs(lines, frequentItems, lines.next().split(' ').collect {
        case x if(frequentItems.itemMapping.contains(x)) => frequentItems.itemMapping(x) }.
        toSet.subsets(2).map( s => s.toIndexedSeq match {
        case Seq(a, b) => if(a < b) (a, b) else (b, a)
      }).foldLeft(counts) { (countsAcc, p) =>
        val k = getPairsIndex(p._1, p._2, frequentItems.itemMapping.size)
        countsAcc.updated(k, counts(k) + 1)
      })
    }
  }

  def getPairsIndex(i: Int, j: Int, n: Int) = ((i-1) * (n-i/2.0)).toInt + j - i

  def getTriangularMatrixSize(n: Int) = ((n-1) * n/2.0).toInt + 1

  def getFrequentPairCounts(support: Int, n: Int, items: FrequentItems, counts: IndexedSeq[Int]) = {
    val idxToName = for((k, v) <- items.itemMapping) yield (v, k)
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
    val pairCounts = countFrequentPairs(lines2, items, new Array[Int](getTriangularMatrixSize(items.itemMapping.size)))
    (items, pairCounts)
  }

  def main(args: Array[String]): Unit = {
    println(args.toList)

    val r = countFrequentPairs(args(0), 2)
    getFrequentPairCounts(2, (r._1.itemMapping.size-1)/2, r._1, r._2)
  }

}
