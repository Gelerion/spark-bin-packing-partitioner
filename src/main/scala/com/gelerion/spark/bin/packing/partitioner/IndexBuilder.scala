package com.gelerion.spark.bin.packing.partitioner

import com.gelerion.spark.bin.packing.partitioner.domain.model.{Bookshelf, BookshelfUrl, EbookTfIdf}
import com.gelerion.spark.bin.packing.partitioner.domain.repository.{GutenbergRepository, TfIdfIndexRepository}
import com.gelerion.spark.bin.packing.partitioner.service.TfIdf
import com.gelerion.spark.bin.packing.partitioner.service.library.gutenberg.GutenbergLibrary
import com.gelerion.spark.bin.packing.partitioner.service.library.gutenberg.modifiers.{BookshelvesPersistentWriter, ThroughFileReader}
import com.gelerion.spark.bin.packing.partitioner.service.repartition.Repartitioner
import com.gelerion.spark.bin.packing.partitioner.spark.SparkHolder
import com.gelerion.spark.bin.packing.partitioner.spark.converters.RddToDatasetConverter
import com.gelerion.spark.bin.packing.partitioner.utils.{Args, CLI}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object IndexBuilder extends Logging {

  lazy val spark: SparkSession = SparkHolder.getSpark

  object gutenbergRepository extends GutenbergRepository {
    val gutenbergLibrary = new GutenbergLibrary with ThroughFileReader with BookshelvesPersistentWriter
  }

  val rddConverter = new RddToDatasetConverter
  val indexRepository = new TfIdfIndexRepository

  // in local mode:
  // --local-mode --limit-bookshelves=3 --limit-ebooks-per-bookshelf=5
  // in distributed mode
  // -part-mode=[skew,pack,partition] --index-directory=[s3/hdfs]directory
  def main(args: Array[String]): Unit = {
    Args.set(new CLI(args))
    import spark.implicits._

    // 1. Parse Gutenberg web page
    val bookshelves: Dataset[Bookshelf] = gutenbergRepository.getBookshelves

    // 2. For each ebook resolve url with text
    val ebookUrls = bookshelves.mapPartitions(bookshelves => {
      val gutenbergLibrary = new GutenbergLibrary()
      bookshelves.map(bookshelf => (BookshelfUrl(bookshelf.url), gutenbergLibrary.resolveEbookUrls(bookshelf)))
    })

    // 3. Repartition skewed dataset
    val partitionedEbookUrls = Args.cli.partMode() match {
      case "pack" => Repartitioner.binPacking(ebookUrls).repartition(16)
      case "partition" => Repartitioner.builtin(ebookUrls).repartition(228)
      case "skew" => ebookUrls.rdd
    }

    // 4. Load the texts, heavy operation
    logger.info("*** GETTING TEXTS")
    //bs-texts
    val corpus = partitionedEbookUrls.mapPartitions(iter => {
      val gutenbergLibrary = new GutenbergLibrary()
      iter.map {
        case (bookshelfUrl, ebookUrl) => (bookshelfUrl, gutenbergLibrary.getEbooksTexts(ebookUrl))
      }
    })//.cache() //if proc mode = full

    // 5. Calculate tf idf weights per book, heavy operation
    logger.info("*** RUNNING TF-IDF ON TEXTS")
    val booksTfIdf: RDD[(BookshelfUrl, Seq[EbookTfIdf])] = corpus.map {
      case (bookshelfUrl, ebookTexts) => (bookshelfUrl, TfIdf.calculate(ebookTexts))
    }

    //proc mode full
//    val bookshelfTfidf = corpus
//      .mapValues(ebookTexts => BookshelfText(ebookTexts.map(_.text).mkString(" ")))
//      .map{ case (id, corpus) => (id, TfIdf.calculate(id, corpus) )}

    //several optimizations:
    // 1. bucketBy url, ebook_id
    // 2. store terms as map
    // 3. normalize data - split into two tables ebooks and terms
    indexRepository.writeEbooksIndex(rddConverter.convert(booksTfIdf))
  }

}
