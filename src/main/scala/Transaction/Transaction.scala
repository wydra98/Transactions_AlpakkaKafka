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

object Transaction extends App{

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

}
