package NoTransaction

import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


object ProducerUtil extends App /*extends Runnable*/ {

  implicit val system = akka.actor.ActorSystem("system")
  implicit val materializer: Materializer = ActorMaterializer()

  val config = system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings =
    ProducerSettings(config, new StringSerializer, new StringSerializer)
      .withBootstrapServers("localhost:9092")
      .withProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      .withProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  //override def run(): Unit = {
  var i = 0
  while (true) {

    println(s"\nPARAGON O ID : ${UniqueId.getId() + 1}")
    val done = Source(1 to 10)
      .map { number =>
        ProducerMessage.single(
          new ProducerRecord[String, String]("topic1",
            new Product(UniqueId.getId(), takeProductName(), randomAmount(), randomPrice()).toString)
        )
      }

      .via(Producer.flexiFlow(producerSettings))
      .map {
        case ProducerMessage.Result(metadata, ProducerMessage.Message(record, passThrough)) => {
          s"Wysyłam:${record.value().split(",")(1)} " +
            s"ilość:  ${record.value().split(",")(2)} " +
            s"cena za jeden ${record.value().split(",")(3).toDouble} " +
            s"paragonu o id ${UniqueId.getId()}"
        }
      }
      .runWith(Sink.foreach(println(_)))

    UniqueId.updateId()
    Thread.sleep(2000)
  }
  //}

  def takeProductName(): String = {
    val listOfProduct = Array("makaron", "chleb", "ryż", "cukier", "baton", "woda", "jogurt", "ketchup", "ser", "banan")
    val generator = new scala.util.Random
    listOfProduct(generator.nextInt(10))
  }

  def randomAmount(): Int = {
    val generator = new scala.util.Random
    generator.nextInt(5) + 1
  }

  def randomPrice(): Double = {
    val generator = new scala.util.Random
    BigDecimal(1 + generator.nextDouble() * 9).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
}
