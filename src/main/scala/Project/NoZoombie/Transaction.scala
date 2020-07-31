package Project.NoZoombie

import java.util.concurrent.atomic.AtomicReference

import Project.Model
import Project.Model.ThreadInterrupt
import Project.Properties.ProjectProperties
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer, Transactional}
import akka.kafka.{ProducerMessage, Subscriptions}
import akka.stream.scaladsl.{Merge, RestartSource, Sink, Source}
import akka.stream.{ActorAttributes, Supervision}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._
import scala.io.AnsiColor._

object Transaction extends App {


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
    new Model.Product(30, "banan", 2, 3),
    new Model.Product(31, "łosoś", 5, 10),
    new Model.Product(32, "banan", 2, 3),
    new Model.Product(33, "woda", 3, 2),
    new Model.Product(34, "chleb", 1, 4.60),
    new Model.Product(35, "jogurt", 1, 3.20),
    new Model.Product(36, "ryż", 3, 15),
    new Model.Product(37, "baton", 1, 15),
    new Model.Product(38, "cukier", 1, 2.5),
    new Model.Product(39, "makaron", 4, 1.3),
    new Model.Product(40, "ser", 3, 25),
    new Model.Product(41, "łosoś", 5, 10),
    new Model.Product(42, "banan", 2, 3),
    new Model.Product(43, "woda", 3, 2),
    new Model.Product(44, "chleb", 1, 4.60),
    new Model.Product(45, "jogurt", 1, 3.20),
    new Model.Product(46, "ryż", 3, 15),
    new Model.Product(47, "baton", 1, 15),
    new Model.Product(48, "cukier", 1, 2.5),
    new Model.Product(49, "makaron", 4, 1.3),
    new Model.Product(50, "ser", 3, 25),
    new Model.Product(51, "łosoś", 5, 10),
    new Model.Product(52, "banan", 2, 3),
    new Model.Product(53, "woda", 3, 2),
    new Model.Product(54, "chleb", 1, 4.60),
    new Model.Product(55, "jogurt", 1, 3.20),
    new Model.Product(56, "ryż", 3, 15),
    new Model.Product(57, "baton", 1, 15),
    new Model.Product(58, "cukier", 1, 2.5),
    new Model.Product(59, "makaron", 4, 1.3),
    new Model.Product(60, "banan", 2, 3),
    new Model.Product(61, "łosoś", 5, 10),
    new Model.Product(62, "banan", 2, 3),
    new Model.Product(63, "woda", 3, 2),
    new Model.Product(64, "chleb", 1, 4.60),
    new Model.Product(65, "jogurt", 1, 3.20),
    new Model.Product(66, "ryż", 3, 15),
    new Model.Product(67, "baton", 1, 15),
    new Model.Product(68, "cukier", 1, 2.5),
    new Model.Product(69, "makaron", 4, 1.3),
    new Model.Product(70, "ser", 3, 25),
    new Model.Product(71, "łosoś", 5, 10),
    new Model.Product(72, "banan", 2, 3),
    new Model.Product(73, "woda", 3, 2),
    new Model.Product(74, "chleb", 1, 4.60),
    new Model.Product(75, "jogurt", 1, 3.20),
    new Model.Product(76, "ryż", 3, 15),
    new Model.Product(77, "baton", 1, 15),
    new Model.Product(78, "cukier", 1, 2.5),
    new Model.Product(79, "makaron", 4, 1.3),
    new Model.Product(80, "ser", 3, 25),
    new Model.Product(81, "łosoś", 5, 10),
    new Model.Product(82, "banan", 2, 3),
    new Model.Product(83, "woda", 3, 2),
    new Model.Product(84, "chleb", 1, 4.60),
    new Model.Product(85, "jogurt", 1, 3.20),
    new Model.Product(86, "ryż", 3, 15),
    new Model.Product(87, "baton", 1, 15),
    new Model.Product(88, "cukier", 1, 2.5),
    new Model.Product(89, "makaron", 4, 1.3),
    new Model.Product(90, "banan", 2, 3),
    new Model.Product(91, "łosoś", 5, 10),
    new Model.Product(92, "banan", 2, 3),
    new Model.Product(93, "woda", 3, 2),
    new Model.Product(94, "chleb", 1, 4.60),
    new Model.Product(95, "jogurt", 1, 3.20),
    new Model.Product(96, "ryż", 3, 15),
    new Model.Product(97, "baton", 1, 15),
    new Model.Product(98, "cukier", 1, 2.5),
    new Model.Product(99, "makaron", 4, 1.3),
    new Model.Product(100, "ser", 3, 25),
    new Model.Product(101, "łosoś", 5, 10),
    new Model.Product(102, "banan", 2, 3),
    new Model.Product(103, "woda", 3, 2),
    new Model.Product(104, "chleb", 1, 4.60),
    new Model.Product(105, "jogurt", 1, 3.20),
    new Model.Product(106, "ryż", 3, 15),
    new Model.Product(107, "baton", 1, 15),
    new Model.Product(108, "cukier", 1, 2.5),
    new Model.Product(109, "makaron", 4, 1.3),
    new Model.Product(110, "ser", 3, 25),
    new Model.Product(111, "łosoś", 5, 10),
    new Model.Product(112, "banan", 2, 3),
    new Model.Product(113, "woda", 3, 2),
    new Model.Product(114, "chleb", 1, 4.60),
    new Model.Product(115, "jogurt", 1, 3.20),
    new Model.Product(116, "ryż", 3, 15),
    new Model.Product(117, "baton", 1, 15),
    new Model.Product(118, "cukier", 1, 2.5),
    new Model.Product(119, "makaron", 4, 1.3),
    new Model.Product(120, "banan", 2, 3))


  /** WĄTEK ODPOWIADAJĄCY ZA RZUCANIE BŁĘDU PODCZAS TRANSAKCJI */
  val thread = new ThreadInterrupt()
  new Thread(thread).start()


  /** PRODUCENT WYSYŁAJĄCY PRODUKTY DO TRANSAKCJI */
  val producer = Source(listOfProduct)
    .throttle(1, 0.2.second)
    .map { product =>
      println(f"${WHITE}Send -> ID: ${product.id}%-7s| name: ${product.name}%-9s| amount: ${product.amount}%-3s| price: ${product.price}%-6s${RESET}")

      if (thread.flag) {
        println(s"${RED}Error was thrown. Every change within from last commit will be aborted.${RESET}")
        throw new Throwable()
      }

      ProducerMessage.single(
        new ProducerRecord[String, String]("producerToTransactionNoZoombie",
          product.toString)
      )
    }
    .via(Producer.flexiFlow(ProjectProperties.producerSettings))


  /** TRANSAKCJA PRZESYŁAJĄCA DANE DO KONSUMENTA */
  val innerControl = new AtomicReference[Control](Consumer.NoopControl)
  val transaction = Transactional
    .source(ProjectProperties.consumerSettings_1, Subscriptions.topics("producerToTransactionNoZoombie"))
    .map { msg =>
      val product = msg.record.value().split(",")
      val x = product(2).toDouble * product(3).toDouble
      val price = BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

      println(f"${YELLOW}ReSend <-> ID: ${product(0)}%-4s| name: ${product(1)}%-9s|" +
        f" total price: ${price}%-6s${RESET}")

      if (thread.flag) {
        println(s"${RED}Error was thrown. Every change within from last commit will be aborted.${RESET}")
        throw new Throwable()
      }

      ProducerMessage.single(
        new ProducerRecord[String, String]("transactionToConsumerNoZoombie", msg.record.key,
         s"${product(0)},${product(1)},$price"),msg.partitionOffset)
    }
    .mapMaterializedValue(c => innerControl.set(c))
    .via(Transactional.flow(ProjectProperties.producerTransaction30SecondsSettings, "transaction1"))


  /** KONSUMENT */
  var finalPrice = 0.0
  val consumer = {
    Consumer
      .plainSource(ProjectProperties.consumerSettings_2, Subscriptions.topics("transactionToConsumerNoZoombie"))
      .map((msg) => {
        val product = msg.value().split(",")

        println(f"${CYAN}Receive <- ID: ${product(0)}%-4s| name: ${product(1)}%-9s|" +
          f" total price: ${product(2)}%-6s${RESET}")

        finalPrice += product(2).trim.toDouble
        if (product(0).trim.toInt == 120) {
          println(s"\n${RED}FINAL PRICE: ${BigDecimal(finalPrice).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble}${RESET}")
        }
      })
  }


  /** WYWOŁANIE SOURC-ÓW JEDNOCZEŚNIE WRAZ Z ICH RESTARTEM W RAZIE BŁĘDU */
  val decider: Supervision.Decider = {
    case e: Exception => {
      println(s"${RED}Exception handled, recovering stream: ${e.getMessage} ${RESET}")
      Supervision.Stop
    }
    case _ => Supervision.Stop
  }

  val totalSource = Source.combine(producer, transaction, consumer)(Merge(_))

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