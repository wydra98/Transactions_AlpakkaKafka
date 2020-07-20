package NoTransaction

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

object ConsumerUtil extends App /*extends Runnable*/ {

  implicit val system: ActorSystem = ActorSystem("consumer-sample")
  implicit val materializer: Materializer = ActorMaterializer()

  val config = system.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings =
    ConsumerSettings(config, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      .withProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      .withProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

  val done = Consumer
    .plainSource(consumerSettings, Subscriptions.topics("topic1"))




    .map(record => record.value)
    .map((x) => x.split(",").drop(2).map((y) => y.toDouble))
    .map((x) => x.product)
    .fold[Double](0)(_ + _)
    .runWith(Sink.foreach[Double]((x) => println(s"Stream function: " +
      "final price = " + BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble +
      s" for receipt id: ${}\n")))

}
