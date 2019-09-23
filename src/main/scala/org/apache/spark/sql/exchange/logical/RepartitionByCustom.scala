package org.apache.spark.sql.exchange.logical

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, RepartitionOperation}
import org.apache.spark.sql.exchange.partitioner.InternalTypedPartitioning

case class RepartitionByCustom(child: LogicalPlan,
                               partitioning: InternalTypedPartitioning[_]) extends RepartitionOperation {
  require(partitioning.numPartitions > 0, s"Number of partitions (${partitioning.numPartitions}) must be positive.")
  override def maxRows: Option[Long] = child.maxRows
  override def shuffle: Boolean = true
  override def numPartitions: Int = partitioning.numPartitions
}
