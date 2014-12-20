package fi.markoa.proto.ml.itemsets

import org.scalatest._

import scala.io.Source
import fi.markoa.proto.ml.itemsets.APriori._

class APrioriSpec extends FlatSpec with Matchers {

  "A-Priori" should "count pass 1 frequent-items table" in {
    val fileName = getClass.getResource("/apriori-1.txt").getFile
    val res = APriori(fileName, 2).countFrequentItems()
    val expectedItemCounts = Map("I2" -> 7, "I1" -> 6, "I3" -> 6)
    res.itemCounts.force.toMap should be (expectedItemCounts)
  }

  "A-Priori" should "calculate frequent pairs" in {
    val fileName = getClass.getResource("/apriori-1.txt").getFile
    val pairs = APriori(fileName, 2).countFrequentItems().countFrequentPairs.pairCounts
    val expectedPairCounts = Map(("I2","I1") -> 4, ("I2","I3") -> 4, ("I1","I3") -> 4)
    pairs should be (expectedPairCounts)
  }

}
