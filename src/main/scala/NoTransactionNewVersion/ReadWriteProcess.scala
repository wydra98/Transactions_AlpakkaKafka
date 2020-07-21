package NoTransactionNewVersion

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
      ProducerMessage.single(
        new ProducerRecord[String, String]("transactionToSink", msg.record.value),
        msg.committableOffset
      )
    }
    .toMat(Producer.committableSink(ProjectProperties.producerSettings))(DrainingControl.apply)
    .run()
}


