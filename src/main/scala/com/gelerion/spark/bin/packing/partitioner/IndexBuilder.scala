package com.gelerion.spark.bin.packing.partitioner

import com.gelerion.spark.bin.packing.partitioner.domain.model.{Bookshelf, BookshelfText, BookshelfUrl, EBooksUrls, EbookTfIdf}
import com.gelerion.spark.bin.packing.partitioner.domain.repository.GutenbergRepository
import com.gelerion.spark.bin.packing.partitioner.library.gutenberg.GutenbergLibrary
import com.gelerion.spark.bin.packing.partitioner.library.gutenberg.modifiers.{BookshelvesPersistentWriter, ThroughFileReader}
import com.gelerion.spark.bin.packing.partitioner.service.TfIdf
import com.gelerion.spark.bin.packing.partitioner.service.repartition.Repartitioner
import com.gelerion.spark.bin.packing.partitioner.utils.{Args, CLI, SparkHolder}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{max, sum}

object IndexBuilder extends Logging {

  // in local mode:
  // --limit-bookshelves=3 --limit-ebooks-per-bookshelf=5
  def main(args: Array[String]): Unit = {
    Args.set(new CLI(args))

    val spark = SparkHolder.getSpark
    import spark.implicits._

    object gutenbergRepository extends GutenbergRepository {
      val gutenbergLibrary = new GutenbergLibrary with ThroughFileReader with BookshelvesPersistentWriter
    }

    // 1. Parse Gutenberg web page
    val books: Dataset[Bookshelf] = gutenbergRepository.getBooks()

    // 2. For each ebook resolve url with text
    val ebookUrls = books.mapPartitions(bookshelves => {
      val gutenbergLibrary = new GutenbergLibrary()
      bookshelves.map(bookshelf => (BookshelfUrl(bookshelf.url), gutenbergLibrary.resolveEbookUrls(bookshelf)))
    }).cache()

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
    }).cache()

    // 5. Calculate tf idf weights per book, heavy operation
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
      .agg(sum($"weight").as("total_weight"))
      .orderBy($"total_weight".desc)
      .limit(20)
      .show(false)

//    tfAsDf
//      .select($"ebook_id", $"ebook_title")
//      .where()


//    booksTdIdf.saveAsTextFile("booksTfIdf")
  }

}
