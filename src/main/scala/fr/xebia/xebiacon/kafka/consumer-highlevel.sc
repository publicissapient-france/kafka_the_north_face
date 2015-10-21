import java.util.Properties

import kafka.consumer.{Consumer, ConsumerConfig}

val props = new Properties()
props.put("zookeeper.connect", "127.0.0.1")
props.put("group.id", "xebicon_printer")
props.put("zookeeper.session.timeout.ms", "400")
props.put("zookeeper.sync.time.ms", "200")
props.put("auto.commit.interval.ms", "1000")

val consumer = Consumer.create(new ConsumerConfig(props))

import scala.collection.convert.wrapAsScala._

consumer.createMessageStreams(Map("xebicon" -> 1)).foreach {
  case (topic, streams) => 
    println(s"Reading from topic $topic")
    streams.foreach {stream => 
      stream.iterator().hasNext()
  }
}