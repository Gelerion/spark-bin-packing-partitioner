package com.gelerion.spark.bin.packing.partitioner

import com.gelerion.spark.bin.packing.partitioner.domain.serde.BookshelfSerDe
import com.gelerion.spark.bin.packing.partitioner.domain.{Bookshelf, BookshelfUrl, EBookTerms, EBooksUrls, EbookText}
import com.gelerion.spark.bin.packing.partitioner.library.gutenberg.GutenbergLibrary
import com.gelerion.spark.bin.packing.partitioner.library.gutenberg.modifiers.{BookshelvesPersistentWriter, ThroughFileReader}
import com.gelerion.spark.bin.packing.partitioner.service.{BinPacking, TfIdf}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.{BufferedSource, Codec}
import scala.reflect.io.File

object Example extends Logging {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("bin-packing")
      .master("local[8]")
      .getOrCreate()

    import spark.implicits._
    implicit val bookshelfUrlToString: BookshelfUrl => String = bookshelfUrl => bookshelfUrl.value

    val gutenbergLibrary = new GutenbergLibrary
      with ThroughFileReader
      with BookshelvesPersistentWriter

    logger.info("*** GETTING URLS")
    val bookshelvesRequested = 20
    val ebooksRequested = 1

    val bookshelves: Seq[Bookshelf]= gutenbergLibrary.getBookshelvesWithEbooks
      //limits push down
      .take(bookshelvesRequested)
      .map(bookshelf => bookshelf.copy(ebooks = bookshelf.ebooks.take(ebooksRequested)))

    val books: Dataset[Bookshelf] = spark.createDataset(bookshelves)

    print("Bookshelves")
    books.show()

    //[#tuple[bookshelf-url {:ebooks [[ebook-id ebook-url]] :size total-ebook-size}]]
    val ebookUrls: Dataset[(BookshelfUrl, EBooksUrls)] = books.mapPartitions(bookshelves => {
      val gutenbergLibrary = new GutenbergLibrary()
      bookshelves.map(bookshelf => (BookshelfUrl(bookshelf.url), gutenbergLibrary.resolveEbookUrls(bookshelf)))
    }).cache()


    print("Ebooks urls")

    //part-mode
    // - pack
    // - partition 228
    // - skew

    //partitioner idx
    val packingItems = ebookUrls.map { case (bookshelfUrl, ebookUrls) => (bookshelfUrl, ebookUrls.totalSize.toLong) }.collect()
      .map { case (url, size) => (url.value, size) }
      .toMap

    for (elem <- BinPacking(packingItems).packNBins(3)) {
      println(s"bin $elem")
    }

//    ebookUrls.rdd.partitionBy()


    if (true) return

    ebookUrls.show()

    //TODO part-mods

    logger.info("*** GETTING TEXTS")
    //bs-texts
    val corpus: Dataset[(BookshelfUrl, Seq[EbookText])] = ebookUrls.mapPartitions(iter => {
      val gutenbergLibrary = new GutenbergLibrary()
      iter.map {
        case (bookshelfUrl, ebookUrl) => (bookshelfUrl, gutenbergLibrary.getEbooksTexts(ebookUrl))
      }
    }).cache()

    corpus.show()

    //    logger.info("*** RUNNING TF-IDF ON TEXTS")
    val booksTdIdf = corpus.map {
      case (bookshelfUrl, ebookTexts) => (bookshelfUrl, TfIdf.calculate(ebookTexts))
    }


    for (arr <- booksTdIdf.collect()) {
      println(arr._1)
      println(arr._2)
    }

    //(let [id-and-terms (su/cache (su/map-vals get-terms id-doc-pairs))]
    //[#tuple[bookshelf-url {:ebooks [[ebook-id text]] :size total-ebook-size}]]
//    val terms: Dataset[(BookshelfUrl, Seq[EBookTerms])] = corpus.map {
//      case (bookshelfUrl, ebookTexts) => (bookshelfUrl, TfIdf.getTerms(ebookTexts))
//    }.cache()
//
//    terms.map {
//      case (bookshelfUrl, terms) => (bookshelfUrl, TfIdf.tf(terms))
//    }
  }

}
