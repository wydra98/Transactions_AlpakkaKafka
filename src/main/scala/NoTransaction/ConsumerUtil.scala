package NoTransaction

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

object ConsumerUtil extends App/*extends Runnable*/ {

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
      .runWith(Sink.foreach(record => println(s"Odebrałem:${record.value().split(",")(1)} " +
        s"ilość:${record.value().split(",")(2)} " +
        s"cena za jeden ${record.value().split(",")(3).toDouble} " +
        s"paragonu o id ${record.value().split(",").head}")))
}
