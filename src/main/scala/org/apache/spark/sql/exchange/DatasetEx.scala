package org.apache.spark.sql.exchange

import com.gelerion.spark.bin.packing.partitioner.spark.internall.RepartitionByCustom
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Repartition}

object DatasetEx {

  def partitionBy[T : Encoder](ds: Dataset[T]): Dataset[T] =
    withTypedPlan(ds.sparkSession, RepartitionByCustom(ds.logicalPlan, 2))

  @inline private def withTypedPlan[U : Encoder](sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[U] = {
    Dataset(sparkSession, logicalPlan)
  }

  implicit class DatasetEx[T : Encoder](val underlying: Dataset[T]) {

    def partitionBy: Dataset[T] =
      withTypedPlan(underlying.sparkSession, RepartitionByCustom(underlying.logicalPlan, 2))
  }

}
