package Transaction

import java.util.concurrent.atomic.AtomicReference

import Properties.ProjectProperties
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Transactional}
import akka.kafka.{ProducerMessage, Subscriptions}
import akka.stream.scaladsl.{RestartSource, Sink}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Await
import scala.concurrent.duration._

object Transaction extends App {

  implicit val system: ActorSystem = akka.actor.ActorSystem("system")
  val innerControl = new AtomicReference[Control](Consumer.NoopControl)

  val stream = RestartSource.onFailuresWithBackoff(
    minBackoff = 1.seconds,
    maxBackoff = 30.seconds,
    randomFactor = 0.2
  ) { () =>
    Transactional
      .source(ProjectProperties.consumerSettings, Subscriptions.topics("sourceToTransaction"))
      .map { msg =>
        val product = msg.record.value().split(",")
        println(f"Send:${product(1)}%-9s| price: ${product(3)}%-6s| amount: ${product(2)}%-3s| receiptId: ${product(0)}")
        if (product(4).trim.toInt % 10 == 0) {
          println()
        }
        ProducerMessage.single(new ProducerRecord("transactionToSink", msg.record.key, msg.record.value), msg.partitionOffset)
      }
      // side effect out the `Control` materialized value because it can't be propagated through the `RestartSource`
      .mapMaterializedValue(c => innerControl.set(c))
      .via(Transactional.flow(ProjectProperties.producerTransactionSettings, "producer"))
  }

  stream.runWith(Sink.ignore)

  // Add shutdown hook to respond to SIGTERM and gracefully shutdown stream
  sys.ShutdownHookThread {
    Await.result(innerControl.get.shutdown(), 10.seconds)
  }


  /** 1 - logging
   * Nieobsłużony wyjątek - stream wywali blad, bo wyjatek jest nieobsluzony */
  /* val faultySource = Source(1 to 10).map(e => if(e ==6) throw new RuntimeException else e)
     faultySource.lag("trackingElements").
     to(Sink.ignore)
     .run()  */

  /** 2 - zakonczenie strumienia
   * wyjatek obsluzony dla 6 wyswietli hejo po czym apka sie zakonczy(ale nie wywali błedu!) */
  /* faultySource.recover {
      case _: RuntimeException => "hejo"}
      .log("gracefulSource")
      .to(Sink.ignore)
      .run */

  /** 3 - zastapnie strumienia
   * gdy zaistnieje wyjatek aplikacja zastapi strumen innym strumieniem */
  /* faultySource.recoverWithRetries( 3 (liczba odnowien), {
      case _: RuntimeException => Source(90 to 99)}
      .log("gracefulSource")
      .to(Sink.ignore)
      .run
      OUTPUT => 1,2,3,4,5,90,91,92, ... , 99
      */

  /** 4 - backoff supervisor
   * po kazdym nieobsluzonym wyjątku aplikacja uruchamia od nowa strumien,
   * za kazdym razem dlugosc czasu potrzebna do uruchomienia nowego strumienia
   * rosnie podwójnie, az do momentu gdy osiagnie wartosc maxBackoff */
  /* val restartSource = RestartSource.onFailureWithBackoff(
     minBackoff = 1 second,
     maxBackoff = 30 second,
     randomFactor =0.2 // ochrania przed tym ze kilka componentow chcialo by zrestartowac w tym samym momencie
     (() => {
     val randomNumber = new Random().nextInt(20)
     Source(1 to 10).map(elem => if(elem == randomNumber) throw new RuntimeException else elem)
     })

     restartSource
     .log()
     .to(Sink.ignore)
     .run()
     )
   */

  /** 5 - supervision strategy
   W przypadku wystąpienia 13 mamy rożne warianty przechwytywania błędów
  */
  /* val numbers = scaladsl.Source(1 to 20).map(n => if(n == 13) throw new RuntimeException("bad luck") else n).log("supervision")
     val supervisedNumbers = numbers.withAttributes(ActorAttributes.supervisionStrategy {

    //Resume = skips the faulty element
    //Stop = stop the stream
    //Restart = resume + clears internal state

     case _: Runtime => Resume
     case _ => Stop
  })
  supervisedNumbers.to(Sink.ignore).run()
   */
}
