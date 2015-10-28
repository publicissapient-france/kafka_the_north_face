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

    new ZkClient("127.0.0.1:2181", 10000, 5000, ZKStringSerializer)
  }
  
  def fetchTopicMetadata(zkClient: ZkClient): TopicMetadata = {
    import kafka.admin.AdminUtils

    //TODO STEP_3_2
    ???
  }

  def findPartitionLeader(topicMetadata: TopicMetadata): Map[Int, Option[Broker]] = {
    import kafka.api.PartitionMetadata

    //TODO STEP_3_3
    ???
  }

  def connectTo(leader: Broker): SimpleConsumer = {
    //TODO STEP_3_4
    ???
  }

  def findEarliestOffset(partition: Int, consumer: SimpleConsumer): Long = {
    //TODO STEP_3_5
    def earliestOffsetRequest = OffsetRequest.EarliestTime
    def consumerId = Request.OrdinaryConsumerId
    
    ???
  }

  def findLatestOffset(partition: Int, consumer: SimpleConsumer): Long = {
    //TODO STEP_3_5
    def latestOffsetRequest = OffsetRequest.LatestTime
    def consumerId = Request.OrdinaryConsumerId
    
    ???
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
    def messageMaxSize = 1000
    def numberOfMessage = 1
    def clientId = "xebicon-printer"
    def topic = "xebicon"
    
    //TODO STEP_3_6
    ???
  }

  def consume(partition: Int, fetchReply: FetchResponse) {
    def readBytes(message: Message): String = {
      val content = Array.ofDim[Byte](message.payloadSize)
      message.payload.get(content, 0, message.payloadSize)
      new String(content, "UTF-8")
    }

    //TODO STEP_3_7
    ???
  }

}
