package com.gelerion.spark.bin.packing.partitioner

import com.gelerion.spark.bin.packing.partitioner.library.gutenberg.{Bookshelf, Ebook, GutenbergLibrary}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

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

    val books: Dataset[Bookshelf] = spark.createDataset(gutenbergLibrary.getBookshelvesWithEbooks())
    logger.info("*** GETTING URLS")
    //[#tuple[bookshelf-url {:ebooks [[ebook-id ebook-url]] :size total-ebook-size}]]
    books.map(bookshelf => {
      new GutenbergLibrary().getEbookUrls(bookshelf)
      //generate ebooks url
      Ebook(1, "a")
    })

    books
//    spark.sparkContext
//      .textFile(ebookUrlsPath)
//      .map(line => {
//
//      })
  }

}
