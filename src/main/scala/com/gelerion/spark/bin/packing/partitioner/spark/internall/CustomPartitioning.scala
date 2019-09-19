package com.gelerion.spark.bin.packing.partitioner.spark.internall

import org.apache.spark.sql.catalyst.plans.physical.Partitioning

case class CustomPartitioning(numPartitions: Int) extends Partitioning {

}
