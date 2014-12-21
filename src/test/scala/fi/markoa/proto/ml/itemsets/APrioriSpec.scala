package fi.markoa.proto.ml.itemsets

import org.scalatest._

import scala.io.Source
import fi.markoa.proto.ml.itemsets.APriori._

class APrioriSpec extends FlatSpec with Matchers {

  "A-Priori" should "count pass 1 frequent-items table" in {
    val fileName = getClass.getResource("/apriori-1.txt").getFile
    val res = APriori(fileName, 2).countFrequentItems()
    val expectedItemCounts = Map("I1" -> 6, "I2" -> 7, "I3" -> 6, "I4" -> 2, "I5" -> 2)
    res.itemCounts.force.toMap should be (expectedItemCounts)
  }

  "A-Priori" should "calculate frequent pairs" in {
    val fileName = getClass.getResource("/apriori-1.txt").getFile
    val pairs = APriori(fileName, 2).countFrequentItems().countFrequentPairs.pairCounts.force.toMap
    val expectedPairCounts = Map(("I1", "I2") -> 4, ("I1", "I3") -> 4, ("I1", "I5") -> 2,
      ("I2", "I3") -> 4, ("I2", "I4") -> 2, ("I2", "I5") -> 2)
    pairs should be (expectedPairCounts)
  }

  "A-Priori" should "calculate frequent triples" in {
    val fileName = getClass.getResource("/apriori-1.txt").getFile
    val items = APriori(fileName, 2).countFrequentItems()
    val triples = items.countFrequentPairs.countFrequentTriples
    val expectedTripleCounts = Map(List("I1", "I2", "I3") -> 2, List("I1", "I2", "I5") -> 2)
    triples.tripleCounts should be (expectedTripleCounts)
  }

}
