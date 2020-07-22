package SinkConsumer

import Properties.ProjectProperties
import akka.actor.ActorSystem
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}

import scala.collection.mutable.ArrayBuffer

object SinkConsumer {

  implicit val system: ActorSystem = ActorSystem("consumer-sample")
  implicit val materializer: Materializer = ActorMaterializer()
  val array = ArrayBuffer[(Int, Double, String)]()

  var idReceipt, idRecord = 0
  var productName = ""
  var flag = true

  def main(args: Array[String]): Unit = {
    Consumer
      .plainSource(ProjectProperties.consumerSettings, Subscriptions.topics("transactionToSink"))
      .map(record => record.value)
      .map((x) => {
        val a = x.split(",")
        idRecord = a(0).toInt
        productName = a(1).toString
        x.split(",").drop(2).map((y) => y.toDouble)
      })
      .map((x) => {
        x(0) * x(1)
      })

      .runWith(Sink.foreach((x) => {
        var numberOfId = {
          var number = 0
          for (product <- array) {
            if (product._1 == idRecord)
              number = number + 1
          }
          number
        }

        if (numberOfId < 10) {
          array.addOne(idRecord, x, productName)
          numberOfId = numberOfId + 1
        }

        if (numberOfId == 10) {
          println(s"****************** ReceiptId = $idRecord ******************\n")
          var finalPrice = 0.0
          for (product <- array) {
            if (product._1 == idRecord) {
              println(f"Receive:${product._3}%-9s| total price: " +
                f"${BigDecimal(product._2).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble}%-6s| receiptId: $idRecord")

              finalPrice = finalPrice + BigDecimal(product._2).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
            }
          }
          println(s"\n**************** FINAL PRICE = ${BigDecimal(finalPrice).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble} ****************\n\n")
        }
      }))
  }

}
