package com.gelerion.spark.bin.packing.partitioner.service

import org.apache.logging.log4j.scala.Logging

import scala.collection.immutable.ListMap
import scala.collection.mutable

/**
 * http://www.martinbroadhurst.com/bin-packing.html
 *  - Next Fit (NF)
 *  - First Fit (FF)
 *  - Best Fit (BF)
 *  - Worst Fit (WF)
 *  - First Fit Decreasing (FFD)
 *  - Worst Fit Decreasing (WFD)
 */
case class BinPacking(private val packingItems: Map[String, Long]) extends Logging {
  import BinPacking.tupleToItem

  //example
  //6 groups of people, of group sizes 3,1,6,4,5 and 2 need to fit
  //onto minibuses with capacity 7 but must stay together in their groups.
  //Find the number of minibuses need to pack them un efficiently and so that each group stays together

  //Solving
  // 1. How to find the lower bound for the problem
  // 2. How to perform the first-fit algorithm
  // 3. How to perform the first-fit decreasing algorithm
  // 4. How to perform full-bin packing

  // 1. (lower bound) ->
  //   how many people I have got: 3 + 1 + 6 + 4 + 5 + 2 = 21
  //   divide by bus capacity 21 / 7 = 3 - lower bound, best it could possible be

  // 2. (first-fit) ->
  //   take the groups (3,1,6,4,5,2) as they come and try to fit them into buses whether there are space
  //   3 -> 7[1]
  //   1 -> 7[1]
  //   6 -> won't fit into the 1st bus 7[2]
  //   4 -> 7[3] third bus
  //   5 -> 4[4]
  //   2 -> 7[1] fits into the first bus
  // we got 4 buses, 7 spaces left

  // 3. (first-fit decreasing) ->
  //   order groups from biggest to smallest (6,5,4,3,2,1)
  //   6 -> 7[1], 5 -> 7[2], 4 -> 7[3], 3 -> 7[3], 2 -> 7[2], 1 -> 7[1]
  // we got 3 buses, no space left

  // 4. (full-bin packing) ->
  //   arrange thing into the size of uor containers
  //   3 + 4, 6 + 1, 5 + 2

  type Items = Map[String, Long]
  type SortedItems = ListMap[String, Long]
  type Bins = mutable.MutableList[Bin] //switch to RBTree or HeapTree for faster smallest bin lookups

  /**
   * Simple first fit decreasing algorithm that continues to add to the smallest bin
   * once n bins have been filled to max-size
   */
  def packNBins(nbins: Int) = {
    val bins = pack(sortDecreasing(packingItems), nbins)
    List(bins.map(_.toImmutable))
  }

//  def packNBins(items: Items, nbins: Int) = {
//    pack(sortDecreasing(items), nbins)
//  }

  private def pack(items: SortedItems, nbins: Int) = {
    firstFit(items)(noFitFn = addToSmallestBin(nbins))
  }

  /**
   * if maxSize isn't defined set max size to be equals to the first element size
   */
  def firstFit(items: Items)(implicit noFitFn: (Bins, Bin) => Bins): Bins = {
    logger.debug(s"Max bin size is ${items.head.size}")
    firstFit(items, items.head.size)
  }

  def firstFit(items: Items, maxSize: Long)(implicit noFitFn: (Bins, Bin) => Bins = addNewBinToBins): Bins = {
    val bins = mutable.MutableList[Bin]()
    for (item <- items) {
      selectBin(bins, item, maxSize) match {
        case Some(bin) => bin.add(item)
        case None => noFitFn(bins, Bin(item.size, mutable.Set(item.name)))
      }
    }

    bins
  }

  private def selectBin(bins: Bins, item: Item, maxSize: Long): Option[Bin] = {
    for (bin <- bins) {
      if ((bin.size + item.size) <= maxSize) {
        return Some(bin)
      }
    }
    None
  }

  // --- no fit functions

  private def addToSmallestBin(nbins: Int)(bins: Bins, bin: Bin): Bins = {
    if (bins.size < nbins) addNewBinToBins(bins, bin)
    else selectSmallestBinAndAdd(bins, bin)
  }

  private def selectSmallestBinAndAdd(bins: Bins, bin: Bin): Bins = {
    bins.min.add(bin)
    bins
  }

  private def addNewBinToBins(bins: Bins, bin: Bin): Bins = {
    bins += bin
  }

  // --- Sorting
  private def sortDecreasing(items: Items): SortedItems = {
    ListMap(items.toSeq.sortWith(_._2 > _._2):_ *)
  }
}

object BinPacking {
  implicit def tupleToItem(tuple: (String, Long)): Item = Item(tuple._1, tuple._2)

  def apply(packingItems: Map[String, Long]) = new BinPacking(packingItems)
}

case class Item(name: String, size: Long)

case class ImmutableBin[Keyword](size: Long, items: Set[Keyword])
case class Bin(var size: Long = 0, items: mutable.Set[String] = mutable.Set.empty) extends Ordered[Bin]{
  def add(item: Item): this.type = {
    size += item.size
    items += item.name
    this
  }

  def add(that: Bin): this.type  = {
    size += that.size
    items ++= that.items
    this
  }

  def toImmutable: ImmutableBin[String] = ImmutableBin(size, Set(items))

  /*
   *   - `x < 0` when `this < that`
   *   - `x == 0` when `this == that`
   *   - `x > 0` when  `this > that`
   */
  override def compare(that: Bin): Int = (this.size - that.size).toInt
}

object MainPacking {

  def main(args: Array[String]): Unit = {
    val items: Map[String, Long] = Map("a" -> 1L, "b" -> 4, "c" -> 9, "d" -> 4, "e" -> 1, "f" -> 5,
      "g" -> 8, "h" -> 3, "i" -> 2, "j" -> 5, "k" -> 7, "l" -> 3, "m" -> 2, "n" -> 6)

    val binPacking = new BinPacking(items)

    val sortedItems = ListMap(items.toSeq.sortWith(_._2 > _._2):_ *)
    println(sortedItems)

    for (bins <- binPacking.firstFit(items, 10)) {
      println(bins)
    }

    println("pack")

    for (bins <- binPacking.packNBins(4)) {
      println(bins)
    }
  }
}
