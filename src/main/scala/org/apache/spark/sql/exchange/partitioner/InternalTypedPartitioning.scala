package org.apache.spark.sql.exchange.partitioner

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.catalyst.plans.physical.Partitioning

class InternalTypedPartitioning[T](val repartitioner: TypedRepartitioner[T], deserializer: Expression)
  extends Partitioning with Serializable {

  def getPartitionKey: InternalRow => T = {
    row => {
      val objProj = GenerateSafeProjection.generate(deserializer :: Nil)
      objProj(row).get(0, null).asInstanceOf[T]
    }
  }

  override val numPartitions: Int = repartitioner.numPartitions
}

