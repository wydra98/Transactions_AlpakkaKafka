package Tests

import Model.Product
import Properties.ProjectProperties
import akka.actor.ActorSystem
import akka.kafka.ProducerMessage
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._

object SourceS extends App {

  implicit val system: ActorSystem = akka.actor.ActorSystem("system")

  var listOfProduct = List[Product](
    new Product(1, "łosos", 5, 10),
    new Product(2, "banan", 2, 3),
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
    new Product(29, "makaron", 4, 1.3),
    new Product(30, "ser", 3, 25)
  )

  /** Produce messages to topic which subscribe class "Transaction" */
  var i = 0
  val zoombie = Source
    .tick(1.second, 1.second, ProducerMessage.single(
      new ProducerRecord[String, String]("sourceToNoTransaction", listOfProduct(i).toString)
    ))
    .via(Producer.flexiFlow(ProjectProperties.producerSettings))
    .map {
      case ProducerMessage.Result(_, ProducerMessage.Message(record, _)) => {
        i = i + 1
        val product = record.value().split(",")
        println(f"Send -> productId: ${product(0)}3s| ${product(1)}%-9s| amount: ${product(2)}%-3s | price: ${product(3)}%-6s")
        if (i == 30) {
          println("\n Everything has been sent. Finish.")
          System.exit(0)
        }
      }
    }

  /** Produce messages to topic which subscribe class "TransactionZoombie" */
  var j = 0
  val noZoombie = Source
    .tick(1.second, 1.second, ProducerMessage.single(
      new ProducerRecord[String, String]("sourceToNoTransactionZoombie", listOfProduct(j).toString)
    ))
    .via(Producer.flexiFlow(ProjectProperties.producerSettings))
    .map {
      case ProducerMessage.Result(metadata, ProducerMessage.Message(record, passThrough)) => {
        j = j + 1
        val product = record.value().split(",")
        println(f"Send:${product(1)}%-9s| price: ${product(3)}%-6s| amount: ${product(2)}%-3s| productId: ${product(0)}")
        if (i == 30) {
          println("\n Everything has been sent. Finish.")
          System.exit(0)
        }
      }
    }
}
