package SourceProducent

import Properties.ProjectProperties
import akka.kafka.ProducerMessage
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._

//import scala.concurrent.duration._
object SourceProducentZoombieNoTransaction extends App {

  implicit val system = akka.actor.ActorSystem("system")
  implicit val materializer: Materializer = ActorMaterializer()
  val uniqueId = new UniqueId

  var i = 0
  val done = Source
    .tick(1.second, 1.second, "")
    .map { _ =>
      if (i % 10 == 0 & i > 0) {
        println()
        uniqueId.updateId()
        i = 0
      }
      i = i + 1
      ProducerMessage.single(
        new ProducerRecord[String, String]("sourceToZoombieNoTransaction",
          new Product(uniqueId.getId(), takeProductName(), randomAmount(), randomPrice(), i).toString)
      )
    }
    .via(Producer.flexiFlow(ProjectProperties.producerSettings))
    .map {

      case ProducerMessage.Result(metadata, ProducerMessage.Message(record, passThrough)) => {
        val product = record.value().split(",")
        f"Send:${product(1)}%-9s| price: ${product(3)}%-6s| amount: ${product(2)}%-3s| receiptId: ${product(0)}"
      }
    }
    .runWith(Sink.foreach(println(_)))

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
