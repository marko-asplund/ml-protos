package fi.markoa.proto.ml.itemsets

import scala.annotation.tailrec
import scala.io.Source
import scala.collection.{Set, Iterator}

/*
implementation of the A-Priori algorithm.
@author markoa
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
    def getPairsIndex(i: Int, j: Int, n: Int = frequentItems.size) = ((i-1) * (n-i/2.0)).toInt + j - i

    def count(ij: (Int, Int)) = counts(getPairsIndex(ij._1, ij._2))
    def isFrequent(ij: (Int, Int)) = count(ij._1, ij._2) >= support

    def countFrequentTriples = APriori.this.countFrequentTriples(frequentTriples = FrequentTriples(this))

    def pairCounts = {
      val idxToName = for ((k, v) <- frequentItems.itemMapping) yield (v, k)
      for { i <- Stream.range(1, frequentItems.size)
            j <- Stream.range(2, frequentItems.size+1) if (i < j)
            k = getPairsIndex(i, j) if (counts(k) >= support)
            t = (idxToName(i), idxToName(j))
      } yield (if(t._1 < t._2) t else (t._2, t._1)) -> counts(k)
    }
  }

  object FrequentPairs {
    def apply(items: FrequentItems) =
      new FrequentPairs(items, new Array[Int](getTriangularMatrixSize(items.itemMapping.size)))
    def getTriangularMatrixSize(n: Int) = ((n-1) * n/2.0).toInt + 1
  }

  class FrequentTriples(val frequentPairs: FrequentPairs, val counts: Map[List[Int], Int]) {
    val idxToName = for ((k, v) <- frequentPairs.frequentItems.itemMapping) yield (v, k)
    def tripleCounts = counts.map( p => (p._1.map(idxToName(_)).sorted, p._2) )
  }

  object FrequentTriples {
    def apply(pairs: FrequentPairs) =
      new FrequentTriples(pairs, Map())
  }

  def getLines() = Source.fromFile(inputFileName).getLines().drop(skip)

  @tailrec final def countFrequentItems(lines: Iterator[String] = getLines(), items: FrequentItems = FrequentItems.Initial): FrequentItems = {
    if(!lines.hasNext)
      items.itemMapping.foldLeft(FrequentItems.Initial) {
        case (itemsAcc, (item, idx)) if (items.counts(idx) >= support) => itemsAcc +(item, items.counts(idx))
        case (itemsAcc, _) => itemsAcc
      }
    else
      countFrequentItems(lines, lines.next().split(recordSeparator).foldLeft(items)( (itemsAcc, item) => itemsAcc + item ))
  }

  def generateCandidatePairsInBasket(basket: String, pairs: FrequentPairs) = {
    basket.split(recordSeparator).collect {
      case x if(pairs.frequentItems.contains(x)) => pairs.frequentItems.index(x) }.
      toSet.subsets(2).map( s => s.toIndexedSeq match {
      case Seq(a, b) => if (a < b) (a, b) else (b, a)
    })
  }

  @tailrec final def countFrequentPairs(lines: Iterator[String] = getLines(), pairs: FrequentPairs): FrequentPairs = {
    if (!lines.hasNext) {
      pairs
    } else {
      // iterate items in basket; filter out non-frequent items; generate pairs, order items in pair
      countFrequentPairs(lines, generateCandidatePairsInBasket(lines.next(), pairs).foldLeft(pairs) { (countsAcc, p) =>
        countsAcc + p
      })
    }
  }

  // can be generalized from triplets to k-sets
  @tailrec final def countFrequentTriples(lines: Iterator[String] = getLines(), frequentTriples: FrequentTriples): FrequentTriples = {
    import frequentTriples.frequentPairs
    if(!lines.hasNext) {
      frequentTriples
    } else {
      val c1 = generateCandidatePairsInBasket(lines.next(), frequentPairs).
        filter( p => frequentPairs.isFrequent(p) ).toList
      val itemCounts = c1.flatMap(p => List(p._1, p._2)).groupBy(v => v).mapValues(_.size)
      val c2 = c1.filter( p => itemCounts(p._1) >= 2 && itemCounts(p._2) >= 2)
      val c3 = (for(i <- c2.zipWithIndex;
          j <- c2.zipWithIndex if(i._2 != j._2);
          s = Set(i._1._1, i._1._1, j._1._1, j._1._2) if(s.size == 3);
          if(s.subsets(2).forall( sub => sub.toIndexedSeq match {
            case Seq(a, b) => frequentPairs.isFrequent(if(a < b) (a, b) else (b, a))
          }))
      ) yield(s.toList.sorted)).toSet
      val ft = c3.foldLeft(frequentTriples.counts)( (countsAcc, i) =>
        countsAcc.updated(i, countsAcc.getOrElse(i, 0) + 1)
      )
      countFrequentTriples(lines, new FrequentTriples(frequentTriples.frequentPairs, ft))
    }
  }
}

object APriori {
  def apply(inputFileName: String, support: Int) = {
    new APriori(inputFileName, support, 1, " ")
  }
}