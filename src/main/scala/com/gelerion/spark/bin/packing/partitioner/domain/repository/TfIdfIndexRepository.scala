package com.gelerion.spark.bin.packing.partitioner.domain.repository

import com.gelerion.spark.bin.packing.partitioner.spark.SparkHolder
import com.gelerion.spark.bin.packing.partitioner.spark.converters.RddToDatasetConverter
import com.gelerion.spark.bin.packing.partitioner.utils.Args
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{Dataset, Row, SaveMode}

class TfIdfIndexRepository(val converter: RddToDatasetConverter = new RddToDatasetConverter) extends Logging {

  private lazy val spark = SparkHolder.getSpark

  private lazy val storageDirectory = Args.cli.outputDirectory()
  private lazy val booksIndexPath = s"${storageDirectory}/books"
  private lazy val bookshelvesIndexPath = s"${storageDirectory}/bookshelves"


  def writeEbooksIndex(ebooksTfIdfIndex: Dataset[Row]): Unit = {
    logger.debug(s"Saving books index to $booksIndexPath")
    ebooksTfIdfIndex.write.mode(SaveMode.Overwrite).parquet(booksIndexPath)
  }

  def readEBooksIndex(): Dataset[Row] = {
    spark.read.schema(converter.ebooksTfIdfSchema).parquet(booksIndexPath)
  }

}
