package com.gelerion.spark.bin.packing.partitioner.spark.converters

import com.gelerion.spark.bin.packing.partitioner.domain.model.{BookshelfUrl, EbookTfIdf}
import com.gelerion.spark.bin.packing.partitioner.spark.SparkHolder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row}

class RddToDatasetConverter {
  private lazy val spark = SparkHolder.getSpark
  import spark.implicits._

  val ebooksTfIdfSchema = StructType(Seq(
    StructField("bookshelf_url", StringType),
    StructField("ebook_id", IntegerType),
    StructField("ebook_title", StringType),
    StructField("term", StringType),
    StructField("weight", DoubleType)
  ))

  implicit private val encoder: ExpressionEncoder[Row] = RowEncoder(ebooksTfIdfSchema)

  def convert(ebooksTfIdfIndex: RDD[(BookshelfUrl, Seq[EbookTfIdf])]): Dataset[Row] = {
    spark.createDataset(ebooksTfIdfIndex)
      .flatMap { case (url, idfs) =>
        for {
          ebookTfIdf <- idfs
          termsWeights <- ebookTfIdf.tfidf
        } yield Row(url.value, ebookTfIdf.ebook.id, ebookTfIdf.ebook.title, termsWeights.term, termsWeights.weight)
      }
  }
}
