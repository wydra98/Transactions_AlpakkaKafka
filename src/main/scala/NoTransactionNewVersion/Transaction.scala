package NoTransactionNewVersion

import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ProducerMessage, Subscriptions}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.kafka.clients.producer.ProducerRecord

object Transaction extends App {

  implicit val system = akka.actor.ActorSystem("system")
  implicit val materializer: Materializer = ActorMaterializer()

  Consumer
    .committableSource(TransactionProperties.consumerSettings, Subscriptions.topics("sourceToTransaction"))
    .map { msg =>
      ProducerMessage.single(
        new ProducerRecord[String, String]("transactionToSink", msg.record.value),
        msg.committableOffset
      )
    }
    .toMat(Producer.committableSink(TransactionProperties.producerSettings))(DrainingControl.apply)
    .run()
}


