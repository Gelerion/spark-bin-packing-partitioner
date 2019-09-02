package com.gelerion.spark.bin.packing.partitioner

import com.gelerion.spark.bin.packing.partitioner.domain.repository.TfIdfIndexRepository
import com.gelerion.spark.bin.packing.partitioner.service.TfIdf
import com.gelerion.spark.bin.packing.partitioner.spark.SparkHolder
import com.gelerion.spark.bin.packing.partitioner.utils.{Args, CLI}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

object ScoreQueryRunner extends Logging{

  lazy val spark: SparkSession = SparkHolder.getSpark
  val indexRepository = new TfIdfIndexRepository

  // in distributed mode
  // --is-local-mode=false --output-directory=[s3/hdfs]directory
  // --search-query=how to cook dinner for forty large human people
  def main(args: Array[String]): Unit = {
    Args.set(new CLI(args))
    import spark.implicits._

    val ebooksTfIdfIndex = indexRepository.readEBooksIndex()

    val query: String = Args.cli.searchQuery()
    logger.info(s"Search query $query")

    //score-query
    ebooksTfIdfIndex
      .where($"term".isin(TfIdf.getTerms(query):_ *))
      .groupBy($"ebook_id", $"ebook_title")
      .agg(sum($"weight").as("total_weight"))
      .orderBy($"total_weight".desc)
      .limit(20)
      .show(false)
  }

}
