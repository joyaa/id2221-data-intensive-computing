import java.util.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}

object KafkaWordCount {
  def main(args: Array[String]) {

    val kafkaConf = Map(
	"metadata.broker.list" -> "localhost:9092",
	"zookeeper.connect" -> "localhost:2181",
	"group.id" -> "kafka-spark-streaming",
	"zookeeper.connection.timeout.ms" -> "1000")

  // Create a local StreamingContext with two working thread and batch interval of 2 second
    //https://spark.apache.org/docs/2.0.0/api/java/org/apache/spark/SparkConf.html
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]") //local on 2 cores (threads?)
    //http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint") //for stateful transformations and shorten length of dependency chain

    // if you want to try the receiver-less approach, comment the below line and uncomment the next one
    //https://spark.apache.org/docs/1.3.0/api/java/index.html?org/apache/spark/streaming/kafka/KafkaUtils.html
    val messages = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, Map("avg" -> 1), StorageLevel.MEMORY_ONLY_SER) //?????????
    //val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](<FILL IN>)

    val values = messages.map(_._2.split(",") )//save the value part of the tuple
    val pairs = values.map(x => (x(0), x(1).toInt)) //DUBBELKOLLA, https://www.safaribooksonline.com/library/view/learning-spark/9781449359034/ch04.html


    def mappingFunc(key: String, value: Option[Int], state: State[Map[String, Tuple2[Int,Int]]]): Option[(String, Double)] = {
      if (state.exists) {
        val existingState = state.get

        val existingValue = existingState(key)

        val sum = existingValue._1 + value.getOrElse(0)
        val counter = existingValue._2 + 1

        state.update(existingState + (key -> new Tuple2(sum, counter)))
        return Option(key, (sum / counter).toDouble)

      } else {
        val sum = value.getOrElse(0)
        state.update(Map(key -> new Tuple2(sum, 1)))
        return Option(key,sum.toDouble)
      }
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _)) //Osäkert

    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
