package SinkConsumer

import Properties.ProjectProperties
import akka.actor.ActorSystem
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.kafka.clients.producer.ProducerRecord

object SinkConsumer extends App {

  implicit val system: ActorSystem = ActorSystem("consumer-sample")
  implicit val materializer: Materializer = ActorMaterializer()

  var productName = ""
  var finalPrice: Double = 0
  var idReceipt = 0
  var idRecord = idReceipt

  Consumer
    .plainSource(ProjectProperties.consumerSettings, Subscriptions.topics("transactionToSink"))

    .map(record => record.value)

    .map((x) => {
      val a = x.split(",")
      idRecord = a(0).toInt
      productName = a(1).toString
      x.split(",").drop(2).map((y) => y.toDouble)
    })

    .map((x) => x(0) * x(1))

    .runWith(Sink.foreach((x) => {
      val price = BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

      if (idReceipt != idRecord) {
        idReceipt = idRecord
        val finalPriceTemp = finalPrice
        println(s"\n**************** FINAL PRICE = ${finalPriceTemp} ****************\n\n")

        Source.single(finalPriceTemp)
          .map(x => new ProducerRecord[String, String]("FinalPrice",
            s"Receipt id: ${idReceipt - 1}\tFinal price = $x"))
          .runWith(Producer.plainSink(ProjectProperties.producerSettings))
        finalPrice = 0
      }

      println(f"Receive:$productName%-9s| total price: $price%-5s| receiptId: $idReceipt")

      Source.single(price)
        .map(x => new ProducerRecord[String, String]("ProductPrice",
          f"Receive:$productName%-9s| total price: $x%-5s| receiptId: $idReceipt"))
        .runWith(Producer.plainSink(ProjectProperties.producerSettings))

      finalPrice = BigDecimal(finalPrice + price).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    }))
}

