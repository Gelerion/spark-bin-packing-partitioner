package org.apache.spark.sql.exchange.partitioner

import org.apache.spark.Partitioner

abstract class CustomTypedPartitioner(override val numPartitions: Int) extends Partitioner
