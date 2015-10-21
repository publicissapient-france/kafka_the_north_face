import java.lang.management.ManagementFactory
import java.time.Instant
import java.util.concurrent.TimeUnit
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
val zkClient = new ZkClient("127.0.0.1:2181", 10000, 5000, ZKStringSerializer)
val brokers = ZkUtils.getAllBrokersInCluster(zkClient)
val connectionString = brokers.map(_.connectionString).mkString(",")
val props = Map(
  "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
  "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
  "partitioner.class" -> "kafka.producer.DefaultPartitioner",
  "max.request.size" -> "10000",
  "producer.type" -> "sync",
  "bootstrap.servers" -> connectionString,
  "acks" -> "all",
  "retries" -> "3",
  "retry.backoff.ms" -> "500")

import scala.collection.convert.wrapAsJava._

val producer = new KafkaProducer[Any, Any](props)

Stream.continually(Instant.now()).foreach { instant =>
  val averageSystemLoad = ManagementFactory.getOperatingSystemMXBean.getSystemLoadAverage
  producer.send(new ProducerRecord[Any, Any]("xebicon", s"$instant: avg_load: $averageSystemLoad")).get(5, TimeUnit.SECONDS)

  Thread.sleep(1000)
}
zkClient.close()