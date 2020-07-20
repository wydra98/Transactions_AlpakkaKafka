package NoTransactionNewVersion

import NoTransaction.ProducerUtil.{randomAmount, randomPrice, takeProductName}
import NoTransaction.{Product, UniqueId}
import akka.kafka.ProducerMessage
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.kafka.clients.producer.ProducerRecord
import scala.concurrent.duration._

object SourceProducent extends App {
  implicit val system = akka.actor.ActorSystem("system")
  implicit val materializer: Materializer = ActorMaterializer()

  var i = 0
  val done = Source
    .tick(1.second, 1.second, "")
    .map { _ =>
      if (i % 10 == 0 & i > 0) {
        println()
        UniqueId.updateId()
        i = 0
      }
      i = i + 1
      ProducerMessage.single(
        new ProducerRecord[String, String]("topic1",
          new Product(UniqueId.getId(), takeProductName(), randomAmount(), randomPrice()).toString)
      )
    }
    .via(Producer.flexiFlow(Properties.producerSettings))
    .map {
      case ProducerMessage.Result(metadata, ProducerMessage.Message(record, passThrough)) => {
        s"Wysyłam $i produkt:${record.value().split(",")(1)} " +
          s"ilość:${record.value().split(",")(2)} " +
          s"cena za jeden ${record.value().split(",")(3).toDouble} " +
          s"paragonu o id ${UniqueId.getId()}"
      }
    }
    .runWith(Sink.foreach(println(_)))


}
