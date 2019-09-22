package org.apache.spark.sql.exchange

import com.gelerion.spark.bin.packing.partitioner.spark.internall.RepartitionByCustom
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, encoderFor}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Repartition}
import org.apache.spark.sql.exchange.partitioner.{InternalTypedPartitioning, TypedRepartitioner}

import scala.reflect.ClassTag

object DatasetEx {

//  def partitionBy[T : Encoder](ds: Dataset[T]): Dataset[T] =
//    withTypedPlan(ds.sparkSession, RepartitionByCustom(ds.logicalPlan, 2))

  @inline private def withTypedPlan[U : Encoder](sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[U] = {
    Dataset(sparkSession, logicalPlan)
  }

  implicit class DatasetEx[T : ClassTag : Encoder](val underlying: Dataset[T]) {
    private[sql] lazy val exprEnc: ExpressionEncoder[T] = encoderFor[T]
    private[sql] lazy val deserializer: Expression =
       exprEnc.resolveAndBind(underlying.logicalPlan.output, underlying.sparkSession.sessionState.analyzer).deserializer

    def partitionBy(partitioner: TypedRepartitioner[T]): Dataset[T] =
      withTypedPlan(underlying.sparkSession, RepartitionByCustom(underlying.logicalPlan, 2,
        new InternalTypedPartitioning(partitioner, deserializer)))
  }

}
