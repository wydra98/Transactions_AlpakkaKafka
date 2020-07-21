package NoTransaction

import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ProducerMessage, Subscriptions}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.kafka.clients.producer.ProducerRecord

object ReadWriteProcess extends App {

  implicit val system = akka.actor.ActorSystem("system")
  implicit val materializer: Materializer = ActorMaterializer()

  Consumer
    .committableSource(ProjectProperties.consumerSettings, Subscriptions.topics("sourceToTransaction"))
    .map { msg =>
      val product = msg.record.value().split(",")
      println(f"Send:${product(1)}%-9s| price: ${product(3)}%-6s| amount: ${product(2)}%-3s| receiptId: ${product(0)}")
      if(product(4).trim.toInt%10 == 0) {
        println()
      }

      ProducerMessage.single(
        new ProducerRecord[String, String]("transactionToSink", msg.record.value),
        msg.committableOffset
      )
    }
    .toMat(Producer.committableSink(ProjectProperties.producerSettings))(DrainingControl.apply)
    .run()
}


