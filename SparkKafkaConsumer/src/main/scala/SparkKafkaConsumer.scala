import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkKafkaConsumer {

  def main(args: Array[String]): Unit = {

    // Required kafka properties to create Consumer
    val kafkaParams = Map[String, Object](

      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "raj_spark_streaming"

    )


    // SparkConfiguration Object
    val sparkConf = new SparkConf().setAppName("Kakfa Consumer").setMaster("local[2]")

    // SparkContext Object
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // Array of topics
    val topics = Array("topic_name")


    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))


    val data =stream.map(record => (record.value))


    data.print()

    ssc.start()
    ssc.awaitTermination()


  }

}