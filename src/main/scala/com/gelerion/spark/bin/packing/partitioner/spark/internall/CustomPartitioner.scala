package com.gelerion.spark.bin.packing.partitioner.spark.internall

import org.apache.spark.Partitioner

case class CustomPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  override def numPartitions: Int = 2

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => if (key.asInstanceOf[String].startsWith("a")) 0 else 1
  }
}
