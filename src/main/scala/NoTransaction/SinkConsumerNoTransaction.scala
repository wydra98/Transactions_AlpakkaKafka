package NoTransaction

import Properties.ProjectProperties
import akka.actor.ActorSystem
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Sink

object SinkConsumerNoTransaction extends App {

  implicit val system: ActorSystem = ActorSystem("consumer-sample")
  var productName = ""
  var idRecord, globalEpoch, epoch = 0
  var offsets = 0L

  val consumer = Consumer
    .plainSource(ProjectProperties.consumerTransactionSettings, Subscriptions.topics("noTransactionToSink"))
    .map(consumerRecord => {
      println(consumerRecord.offset())
      offsets = consumerRecord.offset()
      val x = consumerRecord.value().split(",")
      idRecord = x(0).toInt
      productName = x(1).toString
      val y= x.drop(2).map((y) => y.toDouble)
      y(0) * y(1)
    })
    .runWith(Sink.foreach((x) => {
      val price = BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      println(f"Receive:$productName%-9s| total price: $price%-6s| receiptId: $idRecord | offset: $offsets")
    }))
}
