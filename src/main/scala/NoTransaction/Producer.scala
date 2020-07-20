package NoTransaction


import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

class Producer extends Runnable {

  implicit val system = akka.actor.ActorSystem("system")
  implicit val materializer: Materializer = ActorMaterializer()

  val config = system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings =
    ProducerSettings(config, new StringSerializer, new StringSerializer)
      .withBootstrapServers("localhost:9092")
      .withProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      .withProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  override def run(): Unit = {
    while(true){
      Source(1 to 10)
        .map(value => new Product(UniqueId.getId(), takeProductName(value - 1), randomAmount(), randomPrice()))
        .map(product => new ProducerRecord[String, String]("topic1", product.toString))
        .runWith(Producer.plainSink(producerSettings))
    }
  }

  def takeProductName(i: Int): String = {
    val listOfProduct = Array("makaron", "chleb", "ryż", "cukier", "baton", "woda", "jogurt", "ketchup", "ser", "banan")
    listOfProduct(i)
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
