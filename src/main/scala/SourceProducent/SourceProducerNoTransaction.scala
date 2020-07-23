package SourceProducent

import Properties.ProjectProperties
import akka.actor.ActorSystem
import akka.kafka.ProducerMessage
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Sink, Source}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._

object SourceProducerNoTransaction extends App {

  implicit val system: ActorSystem = akka.actor.ActorSystem("system")
  val uniqueId = new UniqueId
  val uniqueIdZoombie = new UniqueId

  /** Produce messages to topic which subscribe class "NoTransaction" */
  var i = 0
  val noZoombie = Source
    .tick(1.second, 1.second, "")
    .map { _ =>
      ProducerMessage.single(
        new ProducerRecord[String, String]("sourceToTransaction",
          new Product(uniqueId.updateAndGetId, takeProductName(), randomAmount(), randomPrice()).toString)
      )
    }
    .via(Producer.flexiFlow(ProjectProperties.producerSettings))
    .map {
      case ProducerMessage.Result(metadata, ProducerMessage.Message(record, passThrough)) => {
        val product = record.value().split(",")
        f"Send:${product(1)}%-9s| price: ${product(3)}%-6s| amount: ${product(2)}%-3s| productId: ${product(0)}"
      }
    }


  /** Produce messages to topic which subscribe class "NoTransactionZoombie" */
  var j = 0
  val zoombie = Source
    .tick(1.second, 1.second, "")
    .map { _ =>
      ProducerMessage.single(
        new ProducerRecord[String, String]("sourceToTransaction",
          new Product(uniqueIdZoombie.updateAndGetId, takeProductName(), randomAmount(), randomPrice()).toString)
      )
    }
    .via(Producer.flexiFlow(ProjectProperties.producerSettings))
    .map {
      case ProducerMessage.Result(_, ProducerMessage.Message(record, _)) =>
        val product = record.value().split(",")
        f"Send:${product(1)}%-9s| price: ${product(3)}%-6s| amount: ${product(2)}%-3s| productId: ${product(0)}"
    }

  noZoombie.runWith(Sink.foreach(println(_)))
  zoombie.runWith(Sink.foreach(println(_)))

  def takeProductName(): String = {
    val listOfProduct = Array("makaron", "chleb", "ryż", "cukier", "baton", "woda", "jogurt", "ketchup", "ser", "banan")
    val generator = new scala.util.Random
    listOfProduct(generator.nextInt(10))
  }

  def randomAmount(): Int = {
    val generator = new scala.util.Random
    generator.nextInt(5) + 1
  }

  def randomPrice(): Double = {
    val generator = new scala.util.Random
    BigDecimal(1 + generator.nextDouble() * 9).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
}
