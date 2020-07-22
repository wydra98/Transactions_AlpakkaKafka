package SinkConsumer

import Properties.ProjectProperties
import akka.actor.ActorSystem
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Sink

object SinkConsumer extends App {

  implicit val system: ActorSystem = ActorSystem("consumer-sample")
  var productName = ""
  var finalPrice = 0.0
  var idReceipt = 0
  var idRecord = idReceipt

  val consumer = Consumer
    .plainSource(ProjectProperties.consumerTransactionSettings, Subscriptions.topics("transactionToSink"))
    .map(consumerRecord => {
      val x = consumerRecord.value().split(",")
      idRecord = x(0).toInt
      productName = x(1).toString
      val y= x.drop(2).map((y) => y.toDouble)
      y(0) * y(1)
    })
    .runWith(Sink.foreach((x) => {
      val price = BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      if (idReceipt != idRecord) {
        println(s"\n**************** FINAL PRICE = ${finalPrice} ****************\n\n")
        finalPrice = 0
        idReceipt = idRecord
      }
      println(f"Receive:$productName%-9s| total price: $price%-6s| receiptId: $idReceipt")
      finalPrice = BigDecimal(finalPrice + price).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    }))
}

