package com.gelerion.spark.bin.packing.partitioner.domain.repository

import com.gelerion.spark.bin.packing.partitioner.domain.model.Bookshelf
import com.gelerion.spark.bin.packing.partitioner.service.library.gutenberg.GutenbergLibrary
import com.gelerion.spark.bin.packing.partitioner.spark.SparkHolder
import com.gelerion.spark.bin.packing.partitioner.utils.Args
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.Dataset

trait EbooksLibraryRepository {

  def getBookshelves: Dataset[Bookshelf]

}

abstract class GutenbergRepository extends EbooksLibraryRepository with Logging {
  val gutenbergLibrary: GutenbergLibrary //depends on
  import gutenbergLibrary.implicits._

  override def getBookshelves: Dataset[Bookshelf] = {
    val spark = SparkHolder.getSpark
    import spark.implicits._

    logger.info("*** GETTING URLS")
    val bookshelves: Seq[Bookshelf]= withLimitsPushDown(gutenbergLibrary.getBookshelvesWithEbooks)

    logger.info(s"Total bookshelves count is ${bookshelves.size}")
    //enforce custom parallelism
    spark.createDataset(spark.sparkContext.parallelize(bookshelves, 34))
    //spark.createDataset(bookshelves)
  }

  private def withLimitsPushDown(bookshelves: Seq[Bookshelf]): Seq[Bookshelf] = {
    var result = bookshelves

    if (Args.cli.limitBookshelves.isDefined) {
      logger.debug(s"Requested up to ${Args.cli.limitBookshelves()} bookshelves")
      result = bookshelves.limitBookshelves(Args.cli.limitBookshelves())
    }

    if (Args.cli.limitEbooksPerBookshelf.isDefined) {
      logger.debug(s"Requested up to ${Args.cli.limitEbooksPerBookshelf()} e-books per bookshelf")
      result = result.limitEBooksPerShelf(Args.cli.limitEbooksPerBookshelf())
    }

    result
  }
}