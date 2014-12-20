package fi.markoa.proto.ml.itemsets

import scala.annotation.tailrec
import scala.io.Source
import scala.collection.{Set, Iterator}

/*
implementation of the A-Priori algorithm.
@author markoa

TODO
- use streams with pairCounts?
- implement counting for set size 3
- implement counting for set size k
- test with large data sets
 */
class APriori(inputFileName: String, support: Int, skip: Int, recordSeparator: String) {

  class FrequentItems(val counts: IndexedSeq[Int], val itemMapping: Map[String, Int]) {
    def +(item: String, count: Int = 1) = if(itemMapping.contains(item))
      new FrequentItems(counts.updated(itemMapping(item), counts(itemMapping(item)) + count), itemMapping)
    else
      new FrequentItems(counts :+ count, itemMapping.updated(item, counts.size))
    def count(item: String) = counts(itemMapping(item))
    def size() = itemMapping.size
    def contains(item: String) = itemMapping.contains(item)
    def index(item: String) = itemMapping(item)
    def itemCounts = itemMapping.toStream.map( p => (p._1, counts(p._2)) )
    def countFrequentPairs = APriori.this.countFrequentPairs(pairs = FrequentPairs(this))
  }

  object FrequentItems {
    val Initial = new FrequentItems(Vector(-1), Map())
  }

  class FrequentPairs(val frequentItems: FrequentItems, val counts: IndexedSeq[Int]) {
    def +(p: (Int, Int)) = {
      val k = getPairsIndex(p._1, p._2, frequentItems.size)
      new FrequentPairs(frequentItems, counts.updated(k, counts(k) + 1))
    }
    def getPairsIndex(i: Int, j: Int, n: Int) = ((i-1) * (n-i/2.0)).toInt + j - i
    def pairCounts = {
      val idxToName = for((k, v) <- frequentItems.itemMapping) yield (v, k)
      (for(i <- 1 to (frequentItems.size-1);
           j <- 1 to frequentItems.size if(i < j);
           k = getPairsIndex(i, j, frequentItems.size) if(counts(k) > support))
      yield (idxToName(i), idxToName(j)) -> counts(k)).toMap
    }
  }

  object FrequentPairs {
    def apply(items: FrequentItems) =
      new FrequentPairs(items, new Array[Int](getTriangularMatrixSize(items.itemMapping.size)))
    def getTriangularMatrixSize(n: Int) = ((n-1) * n/2.0).toInt + 1
  }

  def getLines() = Source.fromFile(inputFileName).getLines().drop(skip)

  @tailrec final def countFrequentItems(lines: Iterator[String] = getLines(), items: FrequentItems = FrequentItems.Initial): FrequentItems = {
    if(!lines.hasNext) {
      items.itemMapping.foldLeft(FrequentItems.Initial) {
        case (itemsAcc, (item, idx)) if (items.counts(idx) > support) => itemsAcc + (item, items.counts(idx))
        case (itemsAcc, _) => itemsAcc
      }
    } else {
      countFrequentItems(lines, lines.next().split(' ').foldLeft(items)( (itemsAcc, item) => itemsAcc + item ))
    }
  }

  @tailrec final def countFrequentPairs(lines: Iterator[String] = getLines(), pairs: FrequentPairs): FrequentPairs = {
    if (!lines.hasNext)
      pairs
    else {
      // iterate items in basket; filter out non-frequent items; generate pairs, order items in pair
      countFrequentPairs(lines, lines.next().split(' ').collect {
        case x if(pairs.frequentItems.contains(x)) => pairs.frequentItems.index(x) }.
        toSet.subsets(2).map( s => s.toIndexedSeq match {
        case Seq(a, b) => if(a < b) (a, b) else (b, a)
      }).foldLeft(pairs) { (countsAcc, p) =>
        countsAcc + p
      })
    }
  }

}

object APriori {
  def apply(inputFileName: String, support: Int) = {
    new APriori(inputFileName, support, 1, " ")
  }

  def main(args: Array[String]): Unit = {
    println(args.toList)

    val a = APriori(args(0), 2).countFrequentItems().countFrequentPairs
  }

}