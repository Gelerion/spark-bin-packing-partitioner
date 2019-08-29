package com.gelerion.spark.bin.packing.partitioner

import com.gelerion.spark.bin.packing.partitioner.domain.{Bookshelf, BookshelfText, BookshelfUrl, EBooksUrls, EbookTfIdf}
import com.gelerion.spark.bin.packing.partitioner.library.gutenberg.GutenbergLibrary
import com.gelerion.spark.bin.packing.partitioner.library.gutenberg.modifiers.{BookshelvesPersistentWriter, ThroughFileReader}
import com.gelerion.spark.bin.packing.partitioner.service.TfIdf
import com.gelerion.spark.bin.packing.partitioner.service.repartition.Repartitioner
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.max

object Example extends Logging {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("bin-packing")
      .master("local[8]")
      .getOrCreate()

    import spark.implicits._

    val gutenbergLibrary = new GutenbergLibrary
      with ThroughFileReader
      with BookshelvesPersistentWriter

    logger.info("*** GETTING URLS")
    val bookshelvesRequested = 6
    val ebooksRequested = 12

    val bookshelves: Seq[Bookshelf]= gutenbergLibrary.getBookshelvesWithEbooks
      //limits push down
      .take(bookshelvesRequested)
      .map(bookshelf => bookshelf.copy(ebooks = bookshelf.ebooks.take(ebooksRequested)))

    val books: Dataset[Bookshelf] = spark.createDataset(bookshelves)

    val ebookUrls = books.mapPartitions(bookshelves => {
      val gutenbergLibrary = new GutenbergLibrary()
      bookshelves.map(bookshelf => (BookshelfUrl(bookshelf.url), gutenbergLibrary.resolveEbookUrls(bookshelf)))
    }).cache()


    val partMode = if (args.isEmpty) "pack" else args(0)

    val partitionedEbookUrls: RDD[(BookshelfUrl, EBooksUrls)] = partMode match {
      case "pack" => Repartitioner.binPacking(ebookUrls).repartition(16)
      case "partition" => Repartitioner.builtin(ebookUrls).repartition(228)
      case "skew" => ebookUrls.rdd
    }

    logger.info("*** GETTING TEXTS")
    //bs-texts
    val corpus = partitionedEbookUrls.mapPartitions(iter => {
      val gutenbergLibrary = new GutenbergLibrary()
      iter.map {
        case (bookshelfUrl, ebookUrl) => (bookshelfUrl, gutenbergLibrary.getEbooksTexts(ebookUrl))
      }
    }).cache()

    logger.info("*** RUNNING TF-IDF ON TEXTS")
    val booksTdIdf: RDD[(BookshelfUrl, Seq[EbookTfIdf])] = corpus.map {
      case (bookshelfUrl, ebookTexts) => (bookshelfUrl, TfIdf.calculate(ebookTexts))
    }

    //proc mode full
    val bookshelfTfidf = corpus
      .mapValues(ebookTexts => BookshelfText(ebookTexts.map(_.text).mkString(" ")))
      .map{ case (id, corpus) => (id, TfIdf.calculate(id, corpus) )}


    val schema = StructType(Seq(
      StructField("bookshelf_url", StringType),
      StructField("ebook_id", IntegerType),
      StructField("ebook_title", StringType),
      StructField("term", StringType),
      StructField("weight", DoubleType)
    ))
    val encoder = RowEncoder(schema)

    //let Spark handle serde logic
    val tfAsDf = spark.createDataset(booksTdIdf)
      .flatMap { case (url, idfs) =>
        for {
          ebookTfIdf <- idfs;
          termsWeights <- ebookTfIdf.tfidf
        } yield Row(url.value, ebookTfIdf.ebook.id, ebookTfIdf.ebook.title, termsWeights.term, termsWeights.weight)
      } (encoder).cache()

    //several optimizations:
    // 1. bucketBy url, ebook_id
    // 2. store terms as map
    // 3. normalize data - split into two tables ebooks and terms
    tfAsDf.show(false)
//    tfAsDf.printSchema()

    //score-query
    tfAsDf
      .where($"term".isin(TfIdf.getTerms("how to cook dinner for forty large human people"):_ *))
      .groupBy($"ebook_id", $"ebook_title")
      .agg(max($"weight").as("max_weight"))
      .orderBy($"max_weight".desc)
      .show(false)

//    tfAsDf
//      .select($"ebook_id", $"ebook_title")
//      .where()


//    booksTdIdf.saveAsTextFile("booksTfIdf")
  }

}
