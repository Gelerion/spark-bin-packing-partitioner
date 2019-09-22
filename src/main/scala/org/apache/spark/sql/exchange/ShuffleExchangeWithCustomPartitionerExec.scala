package org.apache.spark.sql.exchange

import com.gelerion.spark.bin.packing.partitioner.spark.internall.{CustomPartitioner, CustomPartitioning}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.exchange.partitioner.{InternalTypedPartitioning, TypedRepartitioner}
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{ShuffledRowRDD, SparkPlan, UnsafeRowSerializer}
import org.apache.spark.util.MutablePair
import org.apache.spark.{Partitioner, ShuffleDependency}

case class ShuffleExchangeWithCustomPartitionerExec(partitioning: Partitioning, child: SparkPlan)
  extends Exchange {

  override lazy val metrics: Map[String, SQLMetric] =
    Map("dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"))

  private val serializer: Serializer = new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  //1. register metrics TODO

  //2. nodeName
  override def nodeName: String = {
    s"Exchange(custom partitioner)"
  }

  //3. outputPartitioning
  /** Specifies how data is partitioned across different nodes in the cluster. */
  override def outputPartitioning: Partitioning = partitioning

  //4. doPrepare()
  /*
  There is no sense in defining adaptive query executor (aka coordinator)
  as custom partitioner aims to distribute the data in the best way
   */

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val shuffleDependency = prepareShuffleDependency()
    preparePostShuffleRDD(shuffleDependency)
  }

  private[exchange] def prepareShuffleDependency(): ShuffleDependency[Int, InternalRow, InternalRow] = {
    ShuffleExchangeWithCustomPartitionerExec
      .prepareShuffleDependency(child.execute(), child.output, partitioning, serializer)
  }

  /**
   * Returns a [[ShuffledRowRDD]] that represents the post-shuffle dataset.
   * This [[ShuffledRowRDD]] is created based on a given [[ShuffleDependency]] and an optional
   * partition start indices array. If this optional array is defined, the returned
   * [[ShuffledRowRDD]] will fetch pre-shuffle partitions based on indices of this array.
   */
  private[exchange] def preparePostShuffleRDD(
        shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow],
        specifiedPartitionStartIndices: Option[Array[Int]] = None): ShuffledRowRDD = {
//    // If an array of partition start indices is provided, we need to use this array
//    // to create the ShuffledRowRDD. Also, we need to update newPartitioning to
//    // update the number of post-shuffle partitions.
//    specifiedPartitionStartIndices.foreach { indices =>
//      assert(newPartitioning.isInstanceOf[HashPartitioning])
//      newPartitioning = UnknownPartitioning(indices.length)
//    }
    new ShuffledRowRDD(shuffleDependency, specifiedPartitionStartIndices)
  }
}

object ShuffleExchangeWithCustomPartitionerExec {

  /**
   * Returns a [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  def prepareShuffleDependency(
      rdd: RDD[InternalRow],
      outputAttributes: Seq[Attribute],
      partitioning: Partitioning,
      serializer: Serializer): ShuffleDependency[Int, InternalRow, InternalRow] = {

    val partitioner: Partitioner = partitioning match {
      case CustomPartitioning(numPartitions) => CustomPartitioner(numPartitions)
      case r: InternalTypedPartitioning[_] => r.repartitioner.createPartitioner
    }

    // 1. SQLConf.get.sortBeforeRepartition TODO
    // 2. needToCopyObjectsBeforeShuffle(part) TODO

    def partitionKeyExtractor(): InternalRow => Any = partitioning match {
      case CustomPartitioning(numPartitions) =>
        row => row.getString(0)
      case r: InternalTypedPartitioning[_] => r.getPartitionKey
      case _ => sys.error(s"Exchange not implemented for $partitioning")
    }

    //3. isOrderSensitive TODO
    /** Spark's internal mapPartitionsWithIndex method that skips closure cleaning. It is a performance API to be used
        carefully only if we are sure that the RDD elements are serializable and don't require closure cleaning. */
    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] =
      rdd.mapPartitionsWithIndexInternal { case (partition, iter) =>
        println("Partition: " + partition)
        val getPartitionKey = partitionKeyExtractor()
        val mutablePair = new MutablePair[Int, InternalRow]()

        iter.map { row => mutablePair.update(partitioner.getPartition(getPartitionKey(row)), row) }
      }

    // Now, we manually create a ShuffleDependency. Because pairs in rddWithPartitionIds
    // are in the form of (partitionId, row) and every partitionId is in the expected range
    // [0, part.numPartitions - 1]. The partitioner of this is a PartitionIdPassthrough.
    val dependency =
    new ShuffleDependency[Int, InternalRow, InternalRow](
      rddWithPartitionIds,
      new PartitionIdPassthrough(partitioner.numPartitions),
      serializer)

    dependency
  }

  private class PartitionIdPassthrough(override val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = key.asInstanceOf[Int]
  }
}

