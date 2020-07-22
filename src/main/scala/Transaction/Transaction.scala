package Transaction

import Properties.ProjectProperties
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.Transactional
import akka.kafka.{ProducerMessage, Subscriptions}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.kafka.clients.producer.ProducerRecord

object Transaction{

  implicit val system = akka.actor.ActorSystem("system")
  implicit val materializer: Materializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {
    Transactional
      .source(ProjectProperties.consumerSettings, Subscriptions.topics("sourceToTransaction"))
      .map { msg =>
        val product = msg.record.value().split(",")
        println(f"Send:${product(1)}%-9s| price: ${product(3)}%-6s| amount: ${product(2)}%-3s| receiptId: ${product(0)}")
        if (product(4).trim.toInt % 10 == 0) {
          println()
        }

        ProducerMessage.single(
          new ProducerRecord[String, String]("transactionToSink", msg.record.value),
          msg.partitionOffset
        )
      }
      .toMat(Transactional.sink(ProjectProperties.producerSettings, "producer"))(DrainingControl.apply)
      .run()
  }
}
