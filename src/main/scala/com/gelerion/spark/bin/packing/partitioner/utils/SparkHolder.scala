package com.gelerion.spark.bin.packing.partitioner.utils

import org.apache.spark.sql.SparkSession

object SparkHolder {
  private var spark: SparkSession = _

  private def configure(cli: CLI): Unit = {
    if (cli.isInLocalMode()) {
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
