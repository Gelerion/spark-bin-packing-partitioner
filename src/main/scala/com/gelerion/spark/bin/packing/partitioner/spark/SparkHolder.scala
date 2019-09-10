package com.gelerion.spark.bin.packing.partitioner.spark

import com.gelerion.spark.bin.packing.partitioner.utils.{Args, CLI}
import org.apache.spark.sql.SparkSession

object SparkHolder {
  private var spark: SparkSession = _

  private def configure(cli: CLI): Unit = {
    if (cli.isLocalMode()) {
      spark = SparkSession
        .builder()
        .appName("bin-packing")
        .master("local[8]")
        .getOrCreate()

      return
    }

    spark = SparkSession.builder().getOrCreate()
  }

  def getSpark: SparkSession = {
    if (spark == null) {
      configure(Args.cli)
    }

    spark
  }

}
