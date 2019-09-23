package org.apache.spark.sql.exchange

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, encoderFor}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.exchange.logical.RepartitionByCustom
import org.apache.spark.sql.exchange.partitioner.{InternalTypedPartitioning, TypedPartitioner}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.reflect.ClassTag

object DatasetEx

object implicits {
  @inline private def withTypedPlan[U : Encoder](sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[U] = {
    Dataset(sparkSession, logicalPlan)
  }

  implicit class DatasetEx[T : ClassTag : Encoder](val underlying: Dataset[T]) {
    private[sql] lazy val exprEnc: ExpressionEncoder[T] = encoderFor[T]
    private[sql] lazy val deserializer: Expression =
      exprEnc.resolveAndBind(underlying.logicalPlan.output, underlying.sparkSession.sessionState.analyzer).deserializer

    def partitionBy(partitioner: TypedPartitioner[T]): Dataset[T] = {
      val partitioning = new InternalTypedPartitioning(partitioner, deserializer)
      withTypedPlan(underlying.sparkSession, RepartitionByCustom(underlying.logicalPlan, partitioning))
    }
  }
}
