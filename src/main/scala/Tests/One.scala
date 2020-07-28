package Tests

import java.util.concurrent.atomic.AtomicReference

import Model.Product
import Properties.ProjectProperties
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer, Transactional}
import akka.kafka.{ProducerMessage, Subscriptions}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ActorAttributes, ClosedShape, Supervision}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._

object One extends App {

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
        new ProducerRecord[String, String]("producentToTransaction",
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


  /** TRANSAKCJA PRZESYŁAJĄCA DANE DO KONSUMENTA, COMMIT ZATWIERDZAMY CO 100 MILISEKUND (W TEJ CZĘSCI POKAŻEMY ZOOMBIE) */
  val innerControl = new AtomicReference[Control](Consumer.NoopControl)
  val transaction = Transactional
    .source(ProjectProperties.consumerSettings, Subscriptions.topics("producentToTransaction"))
    .map { msg =>
      val product = msg.record.value().split(",")
      println(f" ReSend -> ID: ${product(0)}%-3s| name: ${product(1)}%-8s|" +
        f" amount: ${product(2)}%-2s| price: ${product(3)}%-6s| offest: ${msg.partitionOffset.offset}")

      ProducerMessage.single(new ProducerRecord("transactionToConsumer", msg.record.key, msg.record.value),
        msg.partitionOffset)
    }
    .mapMaterializedValue(c => innerControl.set(c))
    .via(Transactional.flow(ProjectProperties.producerTransactionSettings, "transaction1"))


  /** TRANSAKCYJNY KONSUMENT WYSYŁAJĄCY KOŃCOWĄ CENĘ NA ZEWNĘTRZNY TOPIC, COMMIT ZATWIERDZANY CO 10 SEKUND
   * (W TEJ CZĘSCI POKAŻEMY WSZYSTKO ALBO NIC, CZYLI RZUCANIE WĄTKU) */
  var finalPrice = 0.0
  val consumer = {
    Transactional
      .source(ProjectProperties.consumerSettings, Subscriptions.topics("transactionToConsumer"))
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

        ProducerMessage.single(new ProducerRecord[String, String]("finalPriceTopic",
          f"Final price -> ID: ${valueArray(0)}%-3s| price: ${price}%-6s"),
          msg.partitionOffset)
      })
      .mapMaterializedValue(c => innerControl.set(c))
      .via(Transactional.flow(ProjectProperties.producerTransactionSettings, "transaction2"))
  }


  /** GRAF URUCHAMIJĄCY PRODUCENTA, TRANSAKCJĘ, KONSUMENTA RÓWNOLEGLE */
  val singleElementSource = Source.single(1)
  val flow1 = Flow[Int].flatMapMerge(1, int => producer)
  val flow2 = Flow[Int].flatMapMerge(1, int => transaction)
  val flow3 = Flow[Int].flatMapMerge(1, int => consumer)

  val runnableGraph = RunnableGraph
    .fromGraph(GraphDSL.create(flow1, flow2, flow3)(Tuple3.apply) {
      implicit builder =>
        (produce, RWP, consume) =>
          import GraphDSL.Implicits._
          val broadcast = builder.add(Broadcast[Int](3))
          singleElementSource ~> broadcast
          broadcast.out(0) ~> produce ~> Sink.ignore
          broadcast.out(1) ~> RWP ~> Sink.ignore
          broadcast.out(2) ~> consume ~> Sink.ignore
          ClosedShape
    })


  /** OBSŁUGA BŁĘDÓW W GRAFIE (AUTOMATYCZNY RESTART PODCZAS PRZECHWYCENIE BŁĘDU) */
  val decider: Supervision.Decider = {
    case e: Exception =>
      println("Exception handled, recovering stream: " + e.getMessage)
      Supervision.Restart
    case _ => Supervision.Stop
  }
  val withCustomSupervision = runnableGraph.withAttributes(ActorAttributes.supervisionStrategy(decider))


  /** URUCHOMIENIE GRAFU */
  val result = withCustomSupervision.run()

}
