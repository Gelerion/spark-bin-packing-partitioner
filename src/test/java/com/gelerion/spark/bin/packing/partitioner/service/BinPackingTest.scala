package com.gelerion.spark.bin.packing.partitioner.service

import org.scalatest._

class BinPackingTest extends FlatSpec with Matchers {

  behavior of "BinPacking"

  it should "packNBins into 4 equally sized bins" in {
    val items: Map[String, Long] = Map("a" -> 1L, "b" -> 4, "c" -> 9, "d" -> 4, "e" -> 1, "f" -> 5,
      "g" -> 8, "h" -> 3, "i" -> 2, "j" -> 5, "k" -> 7, "l" -> 3, "m" -> 2, "n" -> 6)

    val binPacking = BinPacking(items)
    val packed = binPacking.packNBins(4)
    assert(packed.nbins == 4)
    assert(packed.binSizes.equals(List(15L, 15L, 15L, 15L)))
  }

  it should "pack 6 groups of people, of group sizes 3, 1, 6, 4, 5 and 2 onto minibuses with capacity 7" in {
    //find the number of minibuses need to pack them un efficiently and so that each group stays together
    val groupSizes = Map(1 -> 3L, 2 -> 1L, 3 -> 6L, 4 -> 4L, 5 -> 5L, 6 -> 2L)
    val minibusCapacity = 7

    val packed = BinPacking(groupSizes).packMaxCapacity(minibusCapacity)
    assert(packed.nbins == 3)
    assert(packed.binSizes.equals(List(7L, 7L, 7L)))
  }

  it should "create inverted index for items in the pack" in {
    val items: Map[String, Long] = Map("a" -> 1L, "b" -> 4, "c" -> 9, "d" -> 4, "e" -> 1, "f" -> 5,
      "g" -> 8, "h" -> 3, "i" -> 2, "j" -> 5, "k" -> 7, "l" -> 3, "m" -> 2, "n" -> 6)

    val packed = BinPacking(items).packNBins(4)

    assert(packed.lookupItemIdx("a").contains(3))
    assert(packed.lookupItemIdx("c").contains(0))
    assert(packed.lookupItemIdx("m").contains(2))
  }
}
