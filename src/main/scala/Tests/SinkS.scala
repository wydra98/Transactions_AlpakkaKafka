package Tests

import Properties.ProjectProperties
import akka.actor.ActorSystem
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Sink

object SinkS extends App {

  implicit val system: ActorSystem = ActorSystem("consumer-sample")
  var productName = ""
  var idRecord, amount = 0
  var finalPrice, price = 0.0

  val consumer = Consumer
    .plainSource(ProjectProperties.consumerTransactionSettings, Subscriptions.topics("transactionToSink"))
    .map(consumerRecord => {
      val x = consumerRecord.value().split(",")
      idRecord = x(0).toInt
      productName = x(1).toString
      amount = x(2).trim.toInt
      price = x(3).trim.toDouble
      val y = x.drop(2).map((y) => y.toDouble)
      y(0) * y(1)
    })
    .runWith(Sink.foreach((x) => {
      finalPrice += BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      println(f"Receive <- productId: ${idRecord}%-3s| ${productName}%-8s| amount: ${amount}%-2s| price: ${price}%-6s")
      if(idRecord == 30){
        println(s"\n FINAL PRICE: $finalPrice")
      }

    }))
}
