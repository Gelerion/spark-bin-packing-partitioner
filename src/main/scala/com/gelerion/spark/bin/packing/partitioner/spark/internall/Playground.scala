package com.gelerion.spark.bin.packing.partitioner.spark.internall

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, RepartitionOperation}
import org.apache.spark.sql.exchange.ShuffleExchangeWithCustomPartitionerExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{Dataset, SparkSession, Strategy}

object Playground {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("uncle").setMaster("local").set("spark.sql.shuffle.partitions", "2")
    val sparkSession = SparkSession.builder.config(conf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    val spark = sparkSession
    import spark.implicits._
    import org.apache.spark.sql.exchange.DatasetEx.DatasetEx

    spark.experimental.extraStrategies = CustomPartitionerStrategy :: Nil

    val department: Dataset[Dept] = spark.sparkContext.parallelize(
      Seq(
        Dept("a", "ant dept"),
        Dept("d", "duck dept"),
        Dept("c", "cat dept"),
        Dept("r", "rabbit dept"),
        Dept("b", "badger dept"),
        Dept("z", "zebra dept"),
        Dept("m", "mouse dept")
      )
    ).toDS()

    val repartitioned: Dataset[Dept] = department.partitionBy
    repartitioned.show(false)

    println(s"Num Parts: ${repartitioned.rdd.getNumPartitions}")
    repartitioned.rdd.glom().collect().
      zipWithIndex.foreach {
      item: (Array[Dept], Int) =>
        val asList = item._1.mkString
        println(s"array size: ${item._2}. contents: $asList")
    }
  }
}

case class Dept(id: String, value: String)

object CustomPartitionerStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case r: RepartitionByCustom =>
      ShuffleExchangeWithCustomPartitionerExec(CustomPartitioning(2), planLater(r.child)) :: Nil
    // plan could not be applied
    case _ => Nil
  }
}

case class RepartitionByCustom(child: LogicalPlan, numPartitions: Int) extends RepartitionOperation {
  require(numPartitions > 0, s"Number of partitions ($numPartitions) must be positive.")
  override def maxRows: Option[Long] = child.maxRows
  override def shuffle: Boolean = true
}