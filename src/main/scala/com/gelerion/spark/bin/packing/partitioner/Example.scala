package com.gelerion.spark.bin.packing.partitioner

import com.gelerion.spark.bin.packing.partitioner.domain.serde.BookshelfSerDe
import com.gelerion.spark.bin.packing.partitioner.domain.{Bookshelf, BookshelfUrl, EBooksUrls, EbookText, EBookTerms}
import com.gelerion.spark.bin.packing.partitioner.library.gutenberg.GutenbergLibrary
import com.gelerion.spark.bin.packing.partitioner.service.TfIdf
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.{BufferedSource, Codec}
import scala.reflect.io.File

object Example extends Logging {

  def main(args: Array[String]): Unit = {
    val gutenbergLibrary = new GutenbergLibrary()

    val spark: SparkSession = SparkSession
      .builder()
      .appName("bin-packing")
      .master("local[8]")
      .getOrCreate()

    import spark.implicits._

    //or calculate
    val ebookUrlsPath = "/Users/denisshuvalov/Learning/Spark/bin-packing-partitioner/src/main/resources/ebook_urls.txt"

    //val books: Dataset[Bookshelf] = spark.createDataset(gutenbergLibrary.getBookshelvesWithEbooks)

    logger.info("*** GETTING URLS")
    // Shortcut ----
    val requested = 1
    val source: BufferedSource = File("bookshelves").chars(Codec.UTF8)
    val bookshelves: Seq[Bookshelf] = source.getLines().take(requested).flatMap(BookshelfSerDe.decode).flatten.toSeq
    source.close()

    val books: Dataset[Bookshelf] = spark.createDataset(bookshelves)
    // ----

    //val documentsCount = books.flatMap(_.ebooks).count()

    print("Bookshelves")
    books.show()

    //[#tuple[bookshelf-url {:ebooks [[ebook-id ebook-url]] :size total-ebook-size}]]
    val ebookUrls: Dataset[(BookshelfUrl, EBooksUrls)] = books.mapPartitions(bookshelves => {
      val gutenbergLibrary = new GutenbergLibrary()
      bookshelves.map(bookshelf => (BookshelfUrl(bookshelf.url), gutenbergLibrary.resolveEbookUrls(bookshelf)))
    })

    print("Ebooks urls")
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


    corpus.map {
      case (bookshelfUrl, ebookTexts) => (bookshelfUrl, TfIdf.getTerms(ebookTexts))
    }


//    logger.info("*** RUNNING TF-IDF ON TEXTS")
    corpus.show()
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
