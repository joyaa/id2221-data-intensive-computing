import java.util.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import kafka.serializer.{DefaultDecoder, StringDecoder}

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
    val messages = KafkaUtils.createStream[String, String, DefaultDecoder, StringDecoder](ssc, kafkaConf) //?????????+
    //val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](<FILL IN>)

    val values = messages.map(_._2) //save the value part of the typle
    val pairs = values.map((_.split(",")(0), _(1))) //DUBBELKOLLA, https://www.safaribooksonline.com/library/view/learning-spark/9781449359034/ch04.html


    def mappingFunc(key: String, value: Option[Double], state: State[Double]): Option[(String, Double)] = {
/* Förstår ej vad som ska beräknas och vad som finns sparat i state.
      if (state.exists) {

  	     state = state*
         val counter = counter + 1

      } else {
        val initialState = ...
        state.update(initialState)  // Set the initial state
      }
      state.update(average)
       return (sum / counter)
*/
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc)) //Osäkert

    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
