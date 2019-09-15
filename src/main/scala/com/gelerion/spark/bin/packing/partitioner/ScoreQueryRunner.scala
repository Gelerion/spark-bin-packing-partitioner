package com.gelerion.spark.bin.packing.partitioner

import com.gelerion.spark.bin.packing.partitioner.domain.repository.TfIdfIndexRepository
import com.gelerion.spark.bin.packing.partitioner.service.TfIdf
import com.gelerion.spark.bin.packing.partitioner.spark.SparkHolder
import com.gelerion.spark.bin.packing.partitioner.utils.{Args, CLI}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ScoreQueryRunner extends Logging{

  lazy val spark: SparkSession = SparkHolder.getSpark
  val indexRepository = new TfIdfIndexRepository

  // in local mode
  //  --local-mode --search-query="how to cook dinner for forty large human people"
  // in distributed mode
  //  --index-directory=[s3/hdfs]directory --search-query="how to cook dinner for forty large human people"
  def main(args: Array[String]): Unit = {
    Args.set(new CLI(args))
    import spark.implicits._

    val ebooksTfIdfIndex = indexRepository.readEBooksIndex()

    val query: String = Args.cli.searchQuery()
    logger.info(s"Search query $query")

    //1. Calculate similarity level per book within each bookshelf
    //2. Order bookshelves by the total similarity level, so that bookshelf with the most relevant books will be on top
    //3. Within each bookshelf show top 3 most similar books

    //score-query
    //there is no global tf-idf index, it's calculated per bookshelf
    val similarBooks = ebooksTfIdfIndex
      .where($"term".isin(TfIdf.getTerms(query): _ *))
      .groupBy($"bookshelf_url", $"ebook_id", $"ebook_title")
      .agg(sum($"weight").as("similarity_level"))
      .filter($"similarity_level" > 0.001) //filter low relevant books, dynamic value?

    val orderedBySimilarityLevelWindow = Window.partitionBy($"bookshelf_url").orderBy($"similarity_level".desc)
    val bookshelvesWindow = Window.partitionBy($"bookshelf_url")
    val urlPrefixLength = "https://www.gutenberg.org/wiki/".length + 1

//  Bookshelf similarity is calculated based on the sum of the topN similar books
    similarBooks
      .select(
        $"bookshelf_url",
        $"ebook_id",
        substring($"ebook_title", 0, 60).as("title"), //cut long titles
        $"similarity_level",
        rank().over(orderedBySimilarityLevelWindow).as("topN") //order and rank the books by similarity level
      )
      .filter($"topN" <= 2)
      .select($"bookshelf_url", $"ebook_id", $"title", $"similarity_level",
        sum($"similarity_level").over(bookshelvesWindow).as("bookshelfSimilarityLevel"))
      .orderBy($"bookshelfSimilarityLevel".desc, $"bookshelf_url", $"topN")
      .select(
        $"bookshelf_url".substr(urlPrefixLength, urlPrefixLength + 100).as("bookshelf"),
        $"title",
        $"similarity_level")
      .show(10, false)

//    Bookshelf similarity is calculated based on the sum of similarities of each book
//    similarBooks
//      .select(
//        $"bookshelf_url",
//        $"ebook_id",
//        substring($"ebook_title", 0, 60).as("title"), //cut long titles
//        $"similarity_level",
//        rank().over(orderedBySimilarityLevelWindow).as("topN"), //order and rank the books by similarity level
//        sum($"similarity_level").over(bookshelvesWindow).as("bookshelfSimilarityLevel") //get bookshelves with the most similar books
//      )
//      .filter($"topN" <= 3)
//      .orderBy($"bookshelfSimilarityLevel".desc, $"bookshelf_url", $"topN")
//      .select($"bookshelf_url".as("bookshelf"), $"title", $"similarity_level")
//      .show(10, false)
  }
}
