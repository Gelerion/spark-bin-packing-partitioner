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

//      .select($"bookshelf_url", $"ebook_id", $"ebook_title", sum($"weight").over(bookshelf).as("weights"))
//      .orderBy($"bookshelf_url", $"weights".desc)
//      .limit(20)
//      .show(false)


    //score-query
    //there is no global tf-idf index, it's calculated per bookshelf
    val globallyCalculated = ebooksTfIdfIndex
      .where($"term".isin(TfIdf.getTerms(query): _ *))
      .groupBy($"bookshelf_url", $"ebook_id", $"ebook_title")
      .agg(sum($"weight").as("total_weight"))
      .filter($"total_weight" > 0.001) //filter non relevant books
//      .orderBy($"total_weight".desc)
//      .limit(20)
//      .show(false)

    val bookshelf = Window.partitionBy($"bookshelf_url").orderBy($"total_weight".desc)
    val bookshelfGlob = Window.partitionBy($"bookshelf_url")

    //1. Calculate the weight within each bookshelf that is most related to the query
    //2. Order the books withing each bookshelf by the relatness (based on weigh)
    //3. Determine bookshelf with the most related to the query books
    //4. Sort by bookshelf weight so that bookshelf with the most realted books will be the first one etc
    //   and within each category show top 3 books


    globallyCalculated
      .select($"bookshelf_url", $"ebook_id", substring($"ebook_title", 0, 60), $"total_weight",
        rank().over(bookshelf).as("topN"),
        sum($"total_weight").over(bookshelfGlob).as("sum")
      )
      .filter($"topN" <= 3)
      .orderBy($"sum".desc, $"bookshelf_url", $"topN")
      .show(50, false)


//    ebooksTfIdfIndex
//      .where($"bookshelf_url" === "https://www.gutenberg.org/wiki/Cookbooks_and_Cooking_(Bookshelf)")
//      .where($"term".isin(TfIdf.getTerms(query): _ *))
//      .groupBy($"bookshelf_url", $"ebook_id", $"ebook_title")
//      .agg(sum($"weight").as("total_weight"))
//      .orderBy($"total_weight".desc)
//      .limit(20)
//      .show(false)

//    ebooksTfIdfIndex
////      .where($"bookshelf_url".startsWith("https://www.gutenberg.org/wiki/Cookery"))
//      .where($"bookshelf_url" === "https://www.gutenberg.org/wiki/Cookbooks_and_Cooking_(Bookshelf)")
////      .where($"term" === "how")
//      .where($"term".isin(TfIdf.getTerms(query):_ *))
//      .orderBy($"weight".desc)
//      .limit(100)
//      .show(100, false)

//    ebooksTfIdfIndex.where($"ebook_id" === 22829)
//      .orderBy($"weight".desc)
//      .limit(100)
//      .show(100, false)

//    calc.join(ebooksTfIdfIndex, Seq("ebook_id"))
//      .select()

  }

}
