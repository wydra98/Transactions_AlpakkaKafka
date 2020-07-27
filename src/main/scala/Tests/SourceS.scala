package Tests

import Model.Product
import Properties.ProjectProperties
import akka.actor.ActorSystem
import akka.kafka.ProducerMessage
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Sink, Source}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._

object SourceS extends App {

  implicit val system: ActorSystem = akka.actor.ActorSystem("system")

  var listOfProduct = List[Product](
    new Product(0, "banan", 2, 3),
    new Product(1, "łosos", 5, 10),
    new Product(2, "banan", 2, 3),
    new Product(3, "woda", 3, 2),
    new Product(4, "chleb", 1, 4.60),
    new Product(5, "jogrt", 1, 3.20),
    new Product(6, "ryz", 3, 15),
    new Product(7, "baton", 1, 15),
    new Product(8, "cukier", 1, 2.5),
    new Product(9, "makaron", 4, 1.3),
    new Product(10, "ser", 3, 25),
    new Product(11, "łosos", 5, 10),
    new Product(12, "banan", 2, 3),
    new Product(13, "woda", 3, 2),
    new Product(14, "chleb", 1, 4.60),
    new Product(15, "jogrt", 1, 3.20),
    new Product(16, "ryz", 3, 15),
    new Product(17, "baton", 1, 15),
    new Product(18, "cukier", 1, 2.5),
    new Product(19, "makaron", 4, 1.3),
    new Product(20, "ser", 3, 25),
    new Product(21, "łosos", 5, 10),
    new Product(22, "banan", 2, 3),
    new Product(23, "woda", 3, 2),
    new Product(24, "chleb", 1, 4.60),
    new Product(25, "jogrt", 1, 3.20),
    new Product(26, "ryz", 3, 15),
    new Product(27, "baton", 1, 15),
    new Product(28, "cukier", 1, 2.5),
    new Product(29, "makaron", 4, 1.3)
  )

  /** Produce messages to topic which subscribe class "Transaction" */
  var i = 0
  val zoombie = Source
    .tick(1.second, 1.second, "")
    .map { _ =>
      ProducerMessage.single(
        new ProducerRecord[String, String]("sourceToTransaction",
          listOfProduct(i).toString)
      )
    }
    .via(Producer.flexiFlow(ProjectProperties.producerSettings))
    .map {
      case ProducerMessage.Result(_, ProducerMessage.Message(_, _)) => {
        i = i + 1
        f"Send -> productId: ${listOfProduct(i).id}%-3s| name: ${listOfProduct(i).name}%-6s| amount: ${listOfProduct(i).amount}%-3s| price: ${listOfProduct(i).price}%-6s"
      }
    }

  /** Produce messages to topic which subscribe class "TransactionZoombie" */
  var j = 0
  val noZoombie = Source
    .tick(1.second, 1.second, ProducerMessage.single(
      new ProducerRecord[String, String]("sourceToTransactionZoombie", listOfProduct(j).toString)
    ))
    .via(Producer.flexiFlow(ProjectProperties.producerSettings))
    .map {
      case ProducerMessage.Result(_, ProducerMessage.Message(_, _)) => {
        j = j + 1
        f"Send -> productId: ${listOfProduct(j).id}%-3s| name: ${listOfProduct(j).name}%-6s| amount: ${listOfProduct(j).amount}%-3s| price: ${listOfProduct(j).price}%-6s"
      }
    }

    zoombie.runWith(Sink.foreach(println(_)))
    noZoombie.runWith(Sink.foreach(println(_)))
}
