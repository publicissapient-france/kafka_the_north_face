package fr.xebia.xebicon.kafka

import kafka.api._
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.{Message, MessageAndOffset}
import org.I0Itec.zkclient.ZkClient

//TODO STEP_3
object LowLevelConsumer {

  def main(args: Array[String]): Unit = {
    //créer un client Zookeeper
    val zkClient: ZkClient = connectToZookeeper()

    //récupération des métadonnées
    val topicMetadata: TopicMetadata = fetchTopicMetadata(zkClient)

    //recherche du broker leader pour chaque partition
    val partitionsLeaders: Map[Int, Option[Broker]] = findPartitionLeader(topicMetadata)

    partitionsLeaders.foreach {
      case (partition, Some(leader)) =>
        println(s"Consuming from partition $partition")

        consumePartition(partition, leader)

      case (partition, None) =>
        println(s"No leader for partition $partition")
    }
  }

  def connectToZookeeper(): ZkClient = {
    import kafka.utils.ZKStringSerializer

    //TODO STEP_3_1
    new ZkClient("127.0.0.1:2181", 10000, 5000, ZKStringSerializer)
  }

  def fetchTopicMetadata(zkClient: ZkClient): TopicMetadata = {
    import kafka.admin.AdminUtils

    //TODO STEP_3_2
    AdminUtils.fetchTopicMetadataFromZk("xebicon", zkClient)
  }

  def findPartitionLeader(topicMetadata: TopicMetadata): Map[Int, Option[Broker]] = {
    import kafka.api.PartitionMetadata

    def asMap(topicMetadata: TopicMetadata): Map[Int, PartitionMetadata] = {
      topicMetadata.partitionsMetadata.groupBy(_.partitionId).toMap.mapValues(_.head)
    }

    //TODO STEP_3_3
    val partitionsMetadata: Map[Int, PartitionMetadata] = asMap(topicMetadata)

    partitionsMetadata.mapValues(metadata => metadata.leader)
  }

  def connectTo(leader: Broker): SimpleConsumer = {
    //TODO STEP_3_4
    new SimpleConsumer(leader.host, leader.port, 10000, 64000, "xebicon-printer")
  }

  def findEarliestOffset(partition: Int, consumer: SimpleConsumer): Long = {
    //TODO STEP_3_5
    consumer.earliestOrLatestOffset(new TopicAndPartition("xebicon", partition), OffsetRequest.EarliestTime, Request.OrdinaryConsumerId)
  }

  def findLatestOffset(partition: Int, consumer: SimpleConsumer): Long = {
    //TODO STEP_3_5
    consumer.earliestOrLatestOffset(new TopicAndPartition("xebicon", partition), OffsetRequest.LatestTime, Request.OrdinaryConsumerId)
  }

  def consumePartition(partition: Int, leader: Broker): IndexedSeq[Unit] = {
    val consumer = connectTo(leader)
    val earliestOffset = findEarliestOffset(partition, consumer)
    val latestOffset = findLatestOffset(partition, consumer)

    for (offset <- latestOffset.to(earliestOffset, step = -1))
    yield {

      val fetchReply: FetchResponse = requestData(partition, consumer, offset)

      consume(partition, fetchReply)
    }
  }

  def requestData(partition: Int, consumer: SimpleConsumer, offset: Long): FetchResponse = {
    //TODO STEP_3_6
    val request = new FetchRequestBuilder()
      .clientId("xebicon-printer")
      .addFetch("xebicon", partition, offset, 1 * 1000)
      .build()

    consumer.fetch(request)
  }

  def consume(partition: Int, fetchReply: FetchResponse) {
    def readBytes(message: Message): String = {
      val content = Array.ofDim[Byte](message.payloadSize)
      message.payload.get(content, 0, message.payloadSize)
      new String(content, "UTF-8")
    }

    //TODO STEP_3_7
    fetchReply.messageSet("xebicon", partition).iterator.take(1).foreach {
      case MessageAndOffset(message, readOffset) =>
        def offset: Long = readOffset
        val payload: String = readBytes(message)

        println(s"partition: $partition, offset: $offset. $payload")
    }
  }

}
