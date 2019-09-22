package org.apache.spark.sql.exchange.partitioner

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning

import scala.reflect.ClassTag

abstract class TypedRepartitioner[T: ClassTag: Encoder]
       (override val numPartitions: Int) extends Partitioning with Serializable {

  def getPartition(value: T): Int

  def createPartitioner: CustomTypedPartitioner = {
    new CustomTypedPartitioner(numPartitions) {
      override def getPartition(key: Any): Int = TypedRepartitioner.this.getPartition(key.asInstanceOf[T])
    }
  }
}


