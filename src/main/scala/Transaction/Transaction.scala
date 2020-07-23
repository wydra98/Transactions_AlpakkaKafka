package Transaction

import java.util.concurrent.atomic.AtomicReference

import Properties.ProjectProperties
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Transactional}
import akka.kafka.{ProducerMessage, Subscriptions}
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Await
import scala.concurrent.duration._

/** WNIOSEK 1
 * Jeśli nie ma połączenia z kafką OD POCZĄTKU, to Transactional.source automatycznie będzie próbowało się z tą kafką
 * połączyć tak długo aż mu sie nie uda, użytkownik nie ma wpływu na to działanie, strumień nie będzie nic odbierał,
 * ani prztwarzał ani wysyłał, dopóki nie nastąpi połączenie z kafką (jedyny błąd widzimy w logach
 * "java.net.ConnectException: Połączenie odrzucone", natomiast aplikacja jest zamrożona i zatrzymana)
 *
 * WNIOSEK 2
 * Jednak podobnie zachowuje sie aplikacja bez transakcji, wynika z tego ze alpkka generalnie dba o to ze jak nie ma
 * polaczenie w kafce to caly czas prubujemy nawiazac polaczenie dopoki sie nie uda, czyli tutaj brak roznicy miedzy
 * transakcja a jej brakiem
 *
 * WNIOSEK 3
 * Wygląda na to że jak rzucimy jakis wyjątek w środku streamu aplikacja nie zatrzyma się???,
 * przestały wyswietlać się logi Pisało że klient kafki się zatrzymał, nic juz nie odbieramy ani wysyłamy,
 * na szczescie konsumer nie otrzymal wiadomości, które nie zostały zacommitowane
 *
 *
 *
 *
 *
 * */

object Transaction extends App {

  implicit val system: ActorSystem = akka.actor.ActorSystem("system")
  val innerControl = new AtomicReference[Control](Consumer.NoopControl)
  var flag = false
  //@TODO musi działać to tak że jeśli przerwiemy w przed zacomitowaniem to zmiany nie pokażą się w konsumencie,
  //@TODO  pasuje obsłużyć tutaj błędy, jakos wyswietlic to wszystko, napisac ze offset był taki a taki,
  //@TODO a potem był taki, a taki

//  val stream = RestartSource.onFailuresWithBackoff(
//    minBackoff = 1.seconds,
//    maxBackoff = 10.seconds,
//    randomFactor = 0.2
  //) { () =>
    val stream = Transactional
      .source(ProjectProperties.consumerSettings, Subscriptions.topics("sourceToTransaction"))
      .map { msg =>
        val product = msg.record.value().split(",")
        println(f"Send:${product(1)}%-9s| price: ${product(3)}%-6s| amount: ${product(2)}%-3s| productId: ${product(0)}")
        if (product(4).trim.toInt % 10 == 0) {
          println()
        }
//        if (i % 16 == 0)
//          throw new Throwable

        ProducerMessage.single(new ProducerRecord("transactionToSink", msg.record.key, msg.record.value), msg.partitionOffset)
      }

      // side effect out the `Control` materialized value because it can't be propagated through the `RestartSource`
      .mapMaterializedValue(c => innerControl.set(c))
      .via(Transactional.flow(ProjectProperties.producerTransactionSettings, "producer"))
 // }

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
   * W przypadku wystąpienia 13 mamy rożne warianty przechwytywania błędów
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

  //
//  In the javadoc for KafkaConsumer#pause
//  <http://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#pause(java.util.Collection)>
//  it's stated that,
//
//  Suspend fetching from the requested partitions. Future calls to poll(long)
//  > <http://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#poll%28long%29>
//  > will not return any records from these partitions until they have been
//  > resumed using resume(util.Collection)
//  > <http://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#resume%28java.util.Collection%29>.
//  > Note that this method does not affect partition subscription. In
//  > particular, it does not cause a group rebalance when automatic assignment
//    > is used.
//    >
//
//  If a new consumer instance joins the group and the paused partition
//  assigned to this consumer it starts to read data from that partition.  How
//  to pause a partition even if re-balance occurs ?


}
