package fi.markoa.proto.ml.itemsets

import org.scalatest._

import scala.io.Source
import fi.markoa.proto.ml.itemsets.APriori._

class APrioriSpec extends FlatSpec with Matchers {

  "A-Priori" should "count pass 1 frequent-items table" in {
    val is = getClass.getResourceAsStream("/apriori-1.txt")
    val lines = Source.fromInputStream(is).getLines()
    lines.next
    val res = createFrequentItemsTable(lines, 2)
    val counts = res.itemMapping.map { case (k, v) => (k, res.counts(v)) }
    val expectedItemCounts = Map("I2" -> 7, "I1" -> 6, "I3" -> 6)
    counts should be (expectedItemCounts)
  }

  "A-Priori" should "calculate frequent pairs" in {
    val fileName = getClass.getResource("/apriori-1.txt").getFile
    val res = countFrequentPairs(fileName, 2)
    val pairs = getFrequentPairCounts(2, res._1.itemMapping.size, res._1, res._2)
    val expectedPairCounts = Map(("I2","I1") -> 4, ("I2","I3") -> 4, ("I1","I3") -> 4)
    pairs should be (expectedPairCounts)
  }

}
