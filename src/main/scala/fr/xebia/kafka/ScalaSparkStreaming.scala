package fr.xebia.kafka

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object ScalaSparkStreaming {

  def main(args: Array[String]): Unit = {
    val streamingContext = createStreamContext()
    val stream = createStream(streamingContext)

    // TODO Step 7_3
  }

  def createStreamContext(): StreamingContext = {
    // TODO Step 7_1
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("kafka-spark-streaming")

    new StreamingContext(new SparkContext(conf), Seconds(5))
  }

  def createStream(context: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    // TODO Step 7_2
    ???
  }

  def extractValueFromRecord(line: String): Double = {
    val pattern = ".+: avg_load: (.*)".r
    line match {
      case pattern(value) =>
        Try(value)
          .map(_.replace(",", "."))
          .map(_.toDouble)
          .getOrElse(0)
      case _ => 0
    }
  }

  def displayAvg(rdd: RDD[Double]): Unit = {
    val sum = rdd.fold(0)(_ + _)
    val count = rdd.count()
    val avg = if (count == 0) 0 else sum / count

    val timeFormatter = new SimpleDateFormat("HH:mm:ss")

    println(s"${timeFormatter.format(new Date())} : last 5 seconds average load => $avg")
  }

}
