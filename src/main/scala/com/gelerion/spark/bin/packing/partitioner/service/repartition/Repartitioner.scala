package com.gelerion.spark.bin.packing.partitioner.service.repartition

import com.gelerion.spark.bin.packing.partitioner.domain.model.{BookshelfUrl, EBooksUrls}
import com.gelerion.spark.bin.packing.partitioner.service.BinPacking
import com.gelerion.spark.bin.packing.partitioner.spark.partitioner.BinPackingPartitioner
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder}


trait Repartitioner {
  //custom repartition isn't supported by the Dataset API
  def repartition(partitions: Int)
                 (implicit enc: Encoder[(BookshelfUrl, Long)]): RDD[(BookshelfUrl, EBooksUrls)]
}

case class BinPackingRepartitioner(ebookUrls: Dataset[(BookshelfUrl, EBooksUrls)]) extends Repartitioner
  with Logging {

  override def repartition(partitions: Int)
                          (implicit enc: Encoder[(BookshelfUrl, Long)]): RDD[(BookshelfUrl, EBooksUrls)] = {
    val packingItems = ebookUrls.map { case (bookshelfUrl, ebookUrls) => (bookshelfUrl, ebookUrls.totalSize.toLong) }.collect()
      //.map { case (url, size) => (url.value, size) }
      .toMap

    val packedUrlsIntoBins = BinPacking(packingItems).packNBins(partitions)
    logger.debug(s"Packed into ${packedUrlsIntoBins.nbins} bins, sizes ${packedUrlsIntoBins.binSizes}")

    ebookUrls.rdd.partitionBy(new BinPackingPartitioner(packedUrlsIntoBins))
  }
}

case class BuiltinRepartitioner(ebookUrls: Dataset[(BookshelfUrl, EBooksUrls)]) extends Repartitioner {

  override def repartition(partitions: Int)
                          (implicit enc: Encoder[(BookshelfUrl, Long)]): RDD[(BookshelfUrl, EBooksUrls)] = {
    ebookUrls.rdd.repartition(partitions)
  }
}

object Repartitioner {

  def binPacking(ebookUrls: Dataset[(BookshelfUrl, EBooksUrls)]): BinPackingRepartitioner = BinPackingRepartitioner(ebookUrls)

  def builtin(ebookUrls: Dataset[(BookshelfUrl, EBooksUrls)]): BuiltinRepartitioner = BuiltinRepartitioner(ebookUrls)

}

