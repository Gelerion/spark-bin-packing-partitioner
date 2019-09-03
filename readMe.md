![Screenshot](images/gutenberg_library.jpg)

# Overview
This is my adoption to Spark 2.4 of `bin-packing` algorithm initially written in `Clojure` 
by [Silverpond](https://silverpond.com.au/2016/10/06/balancing-spark/) guys.   

Hopefully in a more readable way.  

[Presentation link](https://jsofra.github.io/bin-packing-presentation/)  

# Searching Gutenberg
Given some search terms:  
1. Find the book in each Gutenberg bookshelf that those terms are most important to.
2. Find the Gutenberg bookshelf that those terms are most important to.

# Usage
run index
search query

## What is inside?
1. Distributed Tf Idf Calculation 
2. Distributed Web Crawling and Html Parsing
3. Bin Packing Partitioner
4. Trie Implementation

---
 
#####`Scala` specific stuff
- stackable modifiers
- type aliases
- rational type
- mixin dependency injection

# Data Skew
![Screenshot](images/data_skew.png)  

[Skewed Data](https://blog.clairvoyantsoft.com/optimize-the-skew-in-spark-e523c6ee18ac): Skewness is the statistical 
term, which refers to the value distribution in a given dataset. When we say that data is highly skewed, it means 
some column values have more rows and some very less. E.x. data is not properly/evenly distributed. This affects 
the performance and parallelism in any distributed system.  

Gutenberg library:
 - Inherently skewed data set
 - 228 bookshelves
 - 1 book minimum
 - 1341 book maximum
 - Standard deviation ~ 121


How do you know if the computation is suffering from skewed data?
1. Gangila/Grafana metrics
2. Spark UI
![Screenshot](images/skew_ui_spark.png)  

#Partitioning

####Partition into exact number of bookshelves
 - Overhead from small tasks
 - Poor scheduling
 - Not enough hardware
 - Unused capacity 
 
To create more balanced computation across the workers maybe we can partition the data into more evenly weighted partitions? 
 
## Packing
The problem roughly falls into a class of optimization problems known as packing problems.  
 - https://en.wikipedia.org/wiki/Packing_problems  

|                                |                       |
| ------------------------------ |:---------------------:|
| Set packing                    | Conway puzzle         | 
| Bin packing problem            | Tetris                |     
| Slothouber-Graatsma puzzle     | Covering problem      |  
| Close-packing of equal spheres | Random close pack     | 
| Tetrahedron packing            | Cutting stock problem |
| Kissing number problem         |                       |

These are NP-hard problems so we will need to make use of heuristical methods or 
possibly a constraint solver.  

####Bin Packing
![Screenshot](images/bin_packing.png)   
[image source](http://www.martinbroadhurst.com/bin-packing.html)  
Our problem seems most closely related to the Bin packing problem (minimum number of bins 
of a certain volume) or maybe the Multiprocessor scheduling problem (pack into a specific 
number of bins)  
  
Bin Packing Methods:
1. First Fit (smallest number of fixed sized bins)
2. First Fit + Smallest Bin (fixed number of bins)

Implementation:
```
case class BinPacking[ItemType](packingItems: Map[ItemType, ItemSize]) {
  
  def packNBins(nbins: Int): BinsContainer[ItemType] = pack(sortDecreasing(packingItems), nbins)
  def packMaxCapacity(binMaxCapacity: Int): BinsContainer[ItemType] = firstFit(sortDecreasing(packingItems), binMaxCapacity)
 
  def pack(items: Items, nbins: Int) = firstFit(items)(noFitFn = addToSmallestBin(nbins))
 
  def firstFit(items: Items, maxSize: Long)(implicit noFitFn: (Bins, Bin) => Bins = addNewBinToBins): Bins = {
    val bins = mutable.MutableList[Bin]()
    for (item <- items) {
      selectBin(bins, item, maxSize) match {
        case Some(bin) => bin.add(item)
        case None => noFitFn(bins, MutableBin(item.size, mutable.Set(item.name)))
      }
    }
    bins
  }

  def selectBin(bins: Bins, item: Item, maxSize: Long): Option[MutableBin] = {
    for (bin <- bins) {
      if ((bin.size + item.size) <= maxSize) {
        return Some(bin)
      }
    }
    None
  }
}
```

- Now we could pack items into fixed size bins
```
6 groups of people, of group sizes 3,1,6,4,5 and 2 need to fit
onto minibuses with capacity 7 but must stay together in their groups.
Find the number of minibuses need to pack them un efficiently and so that each group stays together
```

Solution:
```
it should "pack 6 groups of people, of group sizes 3, 1, 6, 4, 5 and 2 onto minibuses with capacity 7" in {
  //find the number of minibuses need to pack them un efficiently and so that each group stays together
  val groupSizes = Map(1 -> 3L, 2 -> 1L, 3 -> 6L, 4 -> 4L, 5 -> 5L, 6 -> 2L)
  val minibusCapacity = 7

  val packed = BinPacking(groupSizes).packMaxCapacity(minibusCapacity)
  assert(packed.nbins == 3)
  assert(packed.binSizes.equals(List(7L, 7L, 7L)))
}
```

- Or to pack into N almost equally sized bins 
```
case class BinPackingRepartitioner(ebookUrls) {
  repartition(partitions: Int): RDD = {
    val packingItems = ebookUrls.map { case (bookshelf, ebooks) => (bookshelf, ebooks.totalTextSize) }.collect().toMap
    
    val packedUrlsIntoBins = BinPacking(packingItems).packNBins(partitions)    
    ebookUrls.rdd.partitionBy(new BinPackingPartitioner(packedUrlsIntoBins))
  }
}

class BinPackingPartitioner(packedUrls: BinsContainer) extends Partitioner {
  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => packedUrls.lookupItemIdx(key).get
  }
}
```
# Term Frequency-Inverse Document Frequency
![Screenshot](images/tf_idf.png) 