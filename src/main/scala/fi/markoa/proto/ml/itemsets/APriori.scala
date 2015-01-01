package fi.markoa.proto.ml.itemsets

import scala.annotation.tailrec
import scala.io.Source
import scala.collection.{Set, Iterator}
import scala.collection.{mutable => M}

/*
implementation of the A-Priori algorithm.
@author markoa
 */
class APriori(inputFileName: String, support: Int, skip: Int, recordSeparator: String) {

  class FrequentItems(val counts: Vector[Int], val itemMapping: Map[String, Int]) {
    def size() = itemMapping.size
    def contains(item: String) = itemMapping.contains(item)
    def index(item: String) = itemMapping(item)
    def itemCounts = itemMapping.toStream.map( p => (p._1, counts(p._2)) )
    def countFrequentPairs = APriori.this.countFrequentPairs(items = this)
  }

  def countFrequentItems(lines: Iterator[String] = getLines()): FrequentItems = {
    class ItemCounts(val counts: M.ArrayBuffer[Int] = M.ArrayBuffer() :+ -1, val itemMapping: M.Map[String, Int] = M.Map()) {
      def +(item: String, count: Int = 1) = {
        if (itemMapping.contains(item)) {
          counts.update(itemMapping(item), counts(itemMapping(item)) + count)
        } else {
          itemMapping.update(item, counts.size)
          counts.append(count)
        }
        this
      }
    }

    @tailrec def doCountFrequentItems(items: ItemCounts): FrequentItems = {
      if (!lines.hasNext) {
        val r = items.itemMapping.foldLeft(new ItemCounts()) {
          case (itemsAcc, (item, idx)) if (items.counts(idx) >= support) => itemsAcc +(item, items.counts(idx))
          case (itemsAcc, _) => itemsAcc
        }
        new FrequentItems(r.counts.toVector, r.itemMapping.toMap)
      } else
        doCountFrequentItems(lines.next().split(recordSeparator).foldLeft(items)((itemsAcc, item) => itemsAcc + item))
    }
    doCountFrequentItems(new ItemCounts())
  }

  class FrequentPairs(val frequentItems: FrequentItems, val counts: Array[Int]) {
    def count(p: IndexedSeq[Int]) = counts(FrequentPairs.getPairsIndex(p, frequentItems.size))
    def isFrequent(p: IndexedSeq[Int]) = count(p) >= support

    def countFrequentTriples = APriori.this.countFrequentTriples(pairs = this)

    def pairCounts = {
      val idxToName = for ((k, v) <- frequentItems.itemMapping) yield (v, k)
      for { i <- Stream.range(1, frequentItems.size)
            j <- Stream.range(2, frequentItems.size+1) if (i < j)
            k = FrequentPairs.getPairsIndex(IndexedSeq(i, j), frequentItems.size) if (counts(k) >= support)
            t = (idxToName(i), idxToName(j))
      } yield (if(t._1 < t._2) t else t.swap) -> counts(k)
    }
  }

  object FrequentPairs {
    def apply(items: FrequentItems) =
      new FrequentPairs(items, new Array[Int](getTriangularMatrixSize(items.itemMapping.size)))
    def getPairsIndex(p: IndexedSeq[Int], n: Int) = ((p(0)-1) * (n-p(0)/2.0)).toInt + p(1) - p(0)
    def getTriangularMatrixSize(n: Int) = ((n-1) * n/2.0).toInt + 1
  }

  private def generateCandidatePairsInBasket(basket: String, items: FrequentItems) = {
    basket.split(recordSeparator).collect {
      case x if(items.contains(x)) => items.index(x)
    }.toSet.subsets(2).map( s => s.toIndexedSeq.sorted )
  }

  def countFrequentPairs(lines: Iterator[String] = getLines(), items: FrequentItems) = {
    @tailrec def doCountFrequentPairs(counts: Array[Int]): FrequentPairs = {
      if (!lines.hasNext) {
        new FrequentPairs(items, counts)
      } else {
        // iterate items in basket; filter out non-frequent items; generate pairs, order items in pair
        doCountFrequentPairs(generateCandidatePairsInBasket(lines.next(), items).foldLeft(counts) { (countsAcc, p) =>
          val k = FrequentPairs.getPairsIndex(p, items.size)
          countsAcc.update(k, countsAcc(k) + 1)
          countsAcc
        })
      }
    }
    doCountFrequentPairs(new Array[Int](FrequentPairs.getTriangularMatrixSize(items.size)))
  }


  class FrequentTriples(val frequentPairs: FrequentPairs, val counts: Map[List[Int], Int]) {
    val idxToName = for ((k, v) <- frequentPairs.frequentItems.itemMapping) yield (v, k)
    def tripleCounts = counts.map( p => (p._1.map(idxToName(_)).sorted, p._2) )
  }

  object FrequentTriples {
    def apply(pairs: FrequentPairs) =
      new FrequentTriples(pairs, Map())
  }

  // can be generalized from triplets to k-sets
  def countFrequentTriples(lines: Iterator[String] = getLines(), pairs: FrequentPairs): FrequentTriples = {
    @tailrec def doCountFrequentTriples(counts: M.Map[List[Int], Int]): FrequentTriples = {
      if(!lines.hasNext) {
        new FrequentTriples(pairs, counts.toMap)
      } else {
        val c1 = generateCandidatePairsInBasket(lines.next(), pairs.frequentItems).
          filter( p => pairs.isFrequent(p) ).toList
        val itemCounts = c1.flatMap(p => p.toList).groupBy(v => v).mapValues(_.size)
        val c2 = c1.filter( p => p.forall(itemCounts(_) >= 2) )
        val c3 = (for(i <- c2.zipWithIndex;
                      j <- c2.zipWithIndex if(i._2 != j._2);
                      s = i._1.toSet ++ j._1 if(s.size == 3);
                      if(s.subsets(2).forall( p => pairs.isFrequent(p.toIndexedSeq.sorted)) )
        ) yield(s.toList.sorted)).toSet
        doCountFrequentTriples(c3.foldLeft(counts)( (countsAcc, i) => {
          countsAcc.update(i, countsAcc.getOrElse(i, 0) + 1)
          countsAcc
        }))
      }
    }
    doCountFrequentTriples(M.Map())
  }

  def getLines() = Source.fromFile(inputFileName).getLines().drop(skip)

}

object APriori {
  def apply(inputFileName: String, support: Int) = {
    new APriori(inputFileName, support, 1, " ")
  }

  def main(args: Array[String]) = {
    val pairs = APriori(args(0), 2).countFrequentItems().countFrequentPairs
    for(p <- pairs.pairCounts)
      println(s"${p._1}: ${p._2}")
  }
}