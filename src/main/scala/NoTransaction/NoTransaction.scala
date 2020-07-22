package NoTransaction

import Properties.ProjectProperties
import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ProducerMessage, Subscriptions}
import org.apache.kafka.clients.producer.ProducerRecord

object NoTransaction extends App {

  implicit val system: ActorSystem = akka.actor.ActorSystem("system")

  val consumer = Consumer
    .committableSource(ProjectProperties.consumerSettings, Subscriptions.topics("sourceToNoTransaction"))
    .map { msg =>
      val product = msg.record.value().split(",")
      println(f"Send:${product(1)}%-9s| price: ${product(3)}%-6s| amount: ${product(2)}%-3s| receiptId: ${product(0)}")
      if (product(4).trim.toInt % 10 == 0) {
        println()
      }
      ProducerMessage.single(
        new ProducerRecord[String, String]("proxyToSink", msg.record.value),
        msg.committableOffset
      )
    }
    .to(Producer.committableSink(ProjectProperties.producerSettings))
    .run()
}
