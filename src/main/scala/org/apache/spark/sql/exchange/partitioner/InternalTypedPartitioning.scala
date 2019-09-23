package org.apache.spark.sql.exchange.partitioner

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.catalyst.plans.physical.Partitioning

private [sql] class InternalTypedPartitioning[T](val partitioner: TypedPartitioner[T], deserializer: Expression)
  extends Partitioning with Serializable {

  def getPartitionKey: InternalRow => T = {
    row => {
      val objProj = GenerateSafeProjection.generate(deserializer :: Nil)
      objProj(row).get(0, null).asInstanceOf[T]
    }
  }

  override val numPartitions: Int = partitioner.numPartitions
}

