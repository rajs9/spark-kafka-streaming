
import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark._


/**
  * Created by dev on 1/4/17.
  */
object SparkKafkaProducer {


  def main(args: Array[String]) {



    val props = new util.HashMap[String, Object]()


    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")




    val sparkConf = new SparkConf().setAppName("Producer").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)


    var lastReadCounter = 0

    val a = 0

    while(true) {


      val data = sc.textFile("hdfs://localhost:9000/readFromMe")


      val datamap = data.map(x => (x.split(",")(0),x))


      // read only updated data from file
      val data2 = datamap.map(x => (

        if (x._1.toInt > lastReadCounter.toInt)
          x._2
        else
          "No New Data"

        )
      )


      // sending data
      data2.foreach(x =>  {


        if(!(x.toString == "No New Data")) {
          val producer = new KafkaProducer[String, String](props)

          val message = new ProducerRecord[String, String]("topic_name",null, x.toString)

          producer.send(message)
        }




      })

     // maintain lastReadCounter
      val lastRead = datamap.reduce((a,b) =>

        if(a._1.toInt > b._1.toInt)
          a
        else
          b

      )


      lastReadCounter = lastRead._1.toInt

      Thread.sleep(10000)


    }





  }


}