package Project.Zoombie

import java.util.concurrent.atomic.AtomicReference

import Project.Model
import Project.Model.ThreadInterrupt
import Project.Properties.ProjectProperties
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ProducerMessage, Subscriptions}
import akka.stream.scaladsl.{Merge, RestartSource, Sink, Source}
import akka.stream.{ActorAttributes, Supervision}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.immutable
import scala.concurrent.duration._
import scala.io.AnsiColor._

object NoTransactionZoombie extends App {

  implicit val system: ActorSystem = akka.actor.ActorSystem("system")


  /** LISTA PRODUKTÓW DO PRZESŁANIA */
  var listOfProduct = List[Model.Product](
    new Model.Product(1, "łosoś", 5, 10),
    new Model.Product(2, "banan", 2, 3),
    new Model.Product(3, "woda", 3, 2),
    new Model.Product(4, "chleb", 1, 4.60),
    new Model.Product(5, "jogurt", 1, 3.20),
    new Model.Product(6, "ryż", 3, 15),
    new Model.Product(7, "baton", 1, 15),
    new Model.Product(8, "cukier", 1, 2.5),
    new Model.Product(9, "makaron", 4, 1.3),
    new Model.Product(10, "ser", 3, 25),
    new Model.Product(11, "łosoś", 5, 10),
    new Model.Product(12, "banan", 2, 3),
    new Model.Product(13, "woda", 3, 2),
    new Model.Product(14, "chleb", 1, 4.60),
    new Model.Product(15, "jogurt", 1, 3.20),
    new Model.Product(16, "ryż", 3, 15),
    new Model.Product(17, "baton", 1, 15),
    new Model.Product(18, "cukier", 1, 2.5),
    new Model.Product(19, "makaron", 4, 1.3),
    new Model.Product(20, "ser", 3, 25),
    new Model.Product(21, "łosoś", 5, 10),
    new Model.Product(22, "banan", 2, 3),
    new Model.Product(23, "woda", 3, 2),
    new Model.Product(24, "chleb", 1, 4.60),
    new Model.Product(25, "jogurt", 1, 3.20),
    new Model.Product(26, "ryż", 3, 15),
    new Model.Product(27, "baton", 1, 15),
    new Model.Product(28, "cukier", 1, 2.5),
    new Model.Product(29, "makaron", 4, 1.3),
    new Model.Product(30, "banan", 2, 3))


  /** WĄTEK ODPOWIADAJĄCY ZA RZUCANIE BŁĘDU PODCZAS TRANSAKCJI */
  val thread = new ThreadInterrupt()
  new Thread(thread).start()


  /** PRODUCENCI WYSYŁAJĄCY PRODUKTY DO TRANSAKCJI */
  val producer = Source(listOfProduct)
    .throttle(1, 1.second)
    .map { product =>{
      println(f"${WHITE}Send -> ID: ${product.id}%-7s| name: ${product.name}%-9s| amount: ${product.amount}%-3s| price: ${product.price}%-6s${RESET}")
      ProducerMessage.multi(
        immutable.Seq(
          new ProducerRecord[String, String]("producerToNoTransaction", product.toString),
          new ProducerRecord[String, String]("producerToNoTransactionZoombie", product.toString)
        )
      )
    }
    }
    .via(Producer.flexiFlow(ProjectProperties.producerSettings))


  /** NIETRANSAKCYJNY POŚREDNIK PRZESYŁAJĄCY DANE DO KONSUMENTA */
  val innerControl = new AtomicReference[Control](Consumer.NoopControl)
  val noTransaction = Consumer
    .committableSource(ProjectProperties.consumerSettings_1, Subscriptions.topics("producerToNoTransaction"))
    .map { msg =>
      val product = msg.record.value().split(",")
      val x = product(2).toDouble * product(3).toDouble
      val price = BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

      println(f"${GREEN}ReSend <-> ID: ${product(0)}%-4s| name: ${product(1)}%-9s|" +
        f" total price: ${price}%-6s| noZoombie${RESET}")

      if (thread.flag) {
        println(s"${RED}Error was thrown. Every change within from last commit will be aborted.${RESET}")
        throw new Throwable()
      }

      Source.single(msg)
        .map(x => new ProducerRecord[String, String]("noTransactionToConsumerZoombie",
          msg.record.value))
        .runWith(Producer.plainSink(ProjectProperties.producerSettings))
    }


  /** NIETRANSAKCYJNY ZOOMBIE  PRZESYŁAJĄCY DANE DO KONSUMENTA */
  val noTransactionZoombie = Consumer
    .committableSource(ProjectProperties.consumerSettings_1, Subscriptions.topics("producerToNoTransactionZoombie"))
    .map { msg =>
      val product = msg.record.value().split(",")
      val x = product(2).toDouble * product(3).toDouble
      val price = BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

      println(f"${MAGENTA}ReSend <-> ID: ${product(0)}%-4s| name: ${product(1)}%-9s|" +
        f" total price: ${price}%-6s| zoombie${RESET}")

      if (thread.flag) {
        println(s"${RED}Error was thrown. Every change within from last commit will be aborted.${RESET}")
        throw new Throwable()
      }

      Source.single(msg)
        .map(x => new ProducerRecord[String, String]("noTransactionToConsumerZoombie",
          msg.record.value))
        .runWith(Producer.plainSink(ProjectProperties.producerSettings))
    }


  /** KONSUMENT */
  var finalPrice = 0.0
  var flag = false
  val consumer = {
    Consumer
      .plainSource(ProjectProperties.consumerSettings_2, Subscriptions.topics("noTransactionToConsumerZoombie"))
      .map((msg) => {
        val product = msg.value().split(",")

        println(f"${CYAN}Receive <- ID: ${product(0)}%-4s| name: ${product(1)}%-9s|" +
          f" total price: ${product(2)}%-6s${RESET}")

        finalPrice += product(2).trim.toDouble
        if (product(0).trim.toInt == 30) {
          if(flag)
          println(s"\n${RED}FINAL PRICE: $finalPrice${RESET}")
          flag = true
        }
      })
  }


  /** WYWOŁANIE SOURC-ÓW JEDNOCZEŚNIE WRAZ Z ICH RESTARTEM W RAZIE BŁĘDU */
  val decider: Supervision.Decider = {
    case e: Exception => {
      println(s"${RED}Exception handled, recovering stream: " + e.getMessage+ s"${RESET}")
      Supervision.Stop
    }
    case _ => Supervision.Stop
  }

  val totalSource = Source.combine(producer, noTransaction, noTransactionZoombie, consumer)(Merge(_))

  RestartSource.onFailuresWithBackoff(
    minBackoff = 1.seconds,
    maxBackoff = 5.seconds,
    randomFactor = 0.2
  ) { () =>
    totalSource
      .withAttributes(ActorAttributes.supervisionStrategy(decider))
  }
    .runWith(Sink.ignore)
}
