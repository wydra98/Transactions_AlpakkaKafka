package Project

import java.util.concurrent.atomic.AtomicReference

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer, Transactional}
import akka.kafka.{ProducerMessage, Subscriptions}
import akka.stream.scaladsl.{Merge, RestartSource, Sink, Source}
import akka.stream.{ActorAttributes, Supervision}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._

object NoTransaction extends App {

  implicit val system: ActorSystem = akka.actor.ActorSystem("system")


  /** LISTA PRODUKTÓW DO PRZESŁANIA */
  var listOfProduct = List[Product](
    new Product(1, "łosoś", 5, 10),
    new Product(2, "banan", 2, 3),
    new Product(3, "woda", 3, 2),
    new Product(4, "chleb", 1, 4.60),
    new Product(5, "jogurt", 1, 3.20),
    new Product(6, "ryż", 3, 15),
    new Product(7, "baton", 1, 15),
    new Product(8, "cukier", 1, 2.5),
    new Product(9, "makaron", 4, 1.3),
    new Product(10, "ser", 3, 25),
    new Product(11, "łosoś", 5, 10),
    new Product(12, "banan", 2, 3),
    new Product(13, "woda", 3, 2),
    new Product(14, "chleb", 1, 4.60),
    new Product(15, "jogurt", 1, 3.20),
    new Product(16, "ryż", 3, 15),
    new Product(17, "baton", 1, 15),
    new Product(18, "cukier", 1, 2.5),
    new Product(19, "makaron", 4, 1.3),
    new Product(20, "ser", 3, 25),
    new Product(21, "łosoś", 5, 10),
    new Product(22, "banan", 2, 3),
    new Product(23, "woda", 3, 2),
    new Product(24, "chleb", 1, 4.60),
    new Product(25, "jogurt", 1, 3.20),
    new Product(26, "ryż", 3, 15),
    new Product(27, "baton", 1, 15),
    new Product(28, "cukier", 1, 2.5),
    new Product(29, "makaron", 4, 1.3),
    new Product(30, "banan", 2, 3))


  /** WĄTEK ODPOWIADAJĄCY ZA RZUCANIE BŁĘDU PODCZAS TRANSAKCJI */
  val thread = new ThreadInterrupt()
  new Thread(thread).start()


  /** PRODUCENT WYSYŁAJĄCY PRODUKTY DO TRANSAKCJI */
  val producer = Source(listOfProduct)
    .throttle(1, 1.second)
    .map { oneProduct =>
      ProducerMessage.single(
        new ProducerRecord[String, String]("producentToProxy",
          oneProduct.toString)
      )
    }
    .via(Producer.flexiFlow(ProjectProperties.producerSettings))
    .map {
      case ProducerMessage.Result(_, ProducerMessage.Message(record, _)) => {
        val product = record.value().split(",")
        println(f"Send -> ID: ${product(0)}%-3s| name: ${product(1)}%-6s| amount: ${product(2)}%-3s| price: ${product(3)}%-6s")
      }
    }


  /** POŚREDNIK NIETRANSAKCYJNY PRZESYŁAJĄCY DANE DO KONSUMENTA, W TYM MIEJSCU POKAŻEMY ŻE ZOOMBIE BEZ TRANSAKCJI PSUJE PROGRAM */
  val innerControl = new AtomicReference[Control](Consumer.NoopControl)
  val proxy = Consumer
    .committableSource(ProjectProperties.consumerSettings_1, Subscriptions.topics("producentToProxy"))
    .map { msg =>
      val product = msg.record.value().split(",")
      println(f" ReSend -> ID: ${product(0)}%-3s| name: ${product(1)}%-8s|" +
        f" amount: ${product(2)}%-2s| price: ${product(3)}%-6s| offest: ${msg.record.offset}")

      Source.single(msg)
        .map(x => new ProducerRecord[String, String]("proxyToConsumer",
          msg.record.value))
        .runWith(Producer.plainSink(ProjectProperties.producerSettings))
    }


  /** NIE TRANSAKCYJNY KONSUMENT WYSYŁAJĄCY KOŃCOWĄ CENĘ NA ZEWNĘTRZNY TOPIC
   * (W TEJ CZĘSCI POKAŻEMY WSZYSTKO ALBO NIC NIE DZIAŁA, CZYLI RZUCANIE WĄTKU) */
  var finalPrice = 0.0
  val consumer = {
    Transactional
      .source(ProjectProperties.consumerSettings_2, Subscriptions.topics("proxyToConsumer"))
      .map((msg) => {
        val valueArray = msg.record.value().split(",")
        val x = valueArray(2).toDouble * valueArray(3).toDouble
        val price = BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

        println(f"    Receive <- ID: ${valueArray(0)}%-3s| ${valueArray(1)}%-8s| amount: ${valueArray(2)}%-2s| price: ${valueArray(3)}%-6s")
        if (thread.flag) {
          println("Error was thrown. Every change within from last commit will be aborted.")
          throw new Throwable()
        }

        finalPrice += price
        if (valueArray(0).trim.toInt == 30) {
          println(s"\n FINAL PRICE: $finalPrice")
        }

        ProducerMessage.single(new ProducerRecord[String, String]("finalPriceTopicNoTransaction",
          f"Final price -> ID: ${valueArray(0)}%-3s| price: ${price}%-6s"),
          msg.partitionOffset)
      })
      .mapMaterializedValue(c => innerControl.set(c))
      .via(Transactional.flow(ProjectProperties.producerTransaction10SecondsSettings, "transaction2"))
  }

  /** WYWOŁANIE SOURC-ÓW JEDNOCZEŚNIE WRAZ Z ICH RESTARTEM W RAZIE BŁĘDU */
  val decider: Supervision.Decider = {
    case e: Exception => {
      println("Exception handled, recovering stream: " + e.getMessage)
      Supervision.Stop
    }
    case _ => Supervision.Stop
  }

  val totalSource = Source.combine(producer, proxy, consumer)(Merge(_))

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
