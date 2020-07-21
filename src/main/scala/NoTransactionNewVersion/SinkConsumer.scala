package NoTransactionNewVersion

import akka.actor.ActorSystem
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SinkConsumer extends App {

  implicit val system: ActorSystem = ActorSystem("consumer-sample")
  implicit val materializer: Materializer = ActorMaterializer()
  val map = new mutable.HashMap[Int,ArrayBuffer[ConsumerRecord[String, String]]]()

  Consumer
    .plainSource(TransactionProperties.consumerSettings, Subscriptions.topics("transactionToSink"))
    .map(record => record.value)
    .map((x) => x.split(",").drop(2).map((y) => y.toDouble))
    .map((x) => x.product)
    .runWith(Sink.foreach((x) => println(x)))
}
