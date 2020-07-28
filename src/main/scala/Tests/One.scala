package Tests

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference

import Model.Product
import Properties.ProjectProperties
import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Producer, Transactional}
import akka.kafka.{ConsumerMessage, ProducerMessage, Subscriptions}
import akka.stream.{ClosedShape, SourceShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, RestartSource, RunnableGraph, Sink, Source}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Future
import scala.concurrent.duration._

object One extends App {
  implicit val system: ActorSystem = akka.actor.ActorSystem("system")

  var listOfProduct = List[Product](
    new Product(1, "łosos", 5, 10),
    new Product(2, "banan", 2, 3),
    new Product(3, "woda", 3, 2),
    new Product(4, "chleb", 1, 4.60),
    new Product(5, "jogrt", 1, 3.20),
    new Product(6, "ryz", 3, 15),
    new Product(7, "baton", 1, 15),
    new Product(8, "cukier", 1, 2.5),
    new Product(9, "makaron", 4, 1.3),
    new Product(10, "ser", 3, 25),
    new Product(11, "łosos", 5, 10),
    new Product(12, "banan", 2, 3),
    new Product(13, "woda", 3, 2),
    new Product(14, "chleb", 1, 4.60),
    new Product(15, "jogrt", 1, 3.20),
    new Product(16, "ryz", 3, 15),
    new Product(17, "baton", 1, 15),
    new Product(18, "cukier", 1, 2.5),
    new Product(19, "makaron", 4, 1.3),
    new Product(20, "ser", 3, 25),
    new Product(21, "łosos", 5, 10),
    new Product(22, "banan", 2, 3),
    new Product(23, "woda", 3, 2),
    new Product(24, "chleb", 1, 4.60),
    new Product(25, "jogrt", 1, 3.20),
    new Product(26, "ryz", 3, 15),
    new Product(27, "baton", 1, 15),
    new Product(28, "cukier", 1, 2.5),
    new Product(29, "makaron", 4, 1.3),
    new Product(30, "banan", 2, 3))


  val thread1 = new ThreadInterrupt()
  val threadProp = new Thread(thread1)
  threadProp.start()

  val firstSource = Source(1 to 1000)

  /** PRODUCENT */
  var i = 0
  val sourceProducer = Source
    .tick(1.second, 1.second, 1)
    .map { _ =>
      ProducerMessage.single(
        new ProducerRecord[String, String]("sourceToTransaction",
          listOfProduct(i).toString)
      )
    }
    .via(Producer.flexiFlow(ProjectProperties.producerSettings))
    .map {
      case ProducerMessage.Result(_, ProducerMessage.Message(_, _)) => {
        i = i + 1
        f"Send -> productId: ${listOfProduct(i).id}%-3s| name: ${listOfProduct(i).name}%-6s| amount: ${listOfProduct(i).amount}%-3s| price: ${listOfProduct(i).price}%-6s"
      }
    }

  /** TRANSACTION */
  val innerControl = new AtomicReference[Control](Consumer.NoopControl)
  val transaction: Source[ProducerMessage.Results[String, String, ConsumerMessage.PartitionOffset], Unit] = Transactional
    .source(ProjectProperties.consumerSettings, Subscriptions.topics("sourceToTransaction"))
    .map { msg =>
      val product = msg.record.value().split(",")
      println(f"Send -> productId: ${product(0)}%-3s| name: ${product(1)}%-8s|" +
        f" amount: ${product(2)}%-2s| price: ${product(3)}%-6s| offest: ${msg.partitionOffset.offset}")
      //        if (product(0).trim().toInt == 15) {
      //          System.err.println("Bład został rzucony podczas wiadomości o id " + product(0).trim().toInt)
      //          throw new Throwable
      //        }
      ProducerMessage.single(new ProducerRecord("transactionToSink", msg.record.key, msg.record.value),
        msg.partitionOffset)
    }
    .mapMaterializedValue(c => innerControl.set(c))
    .via(Transactional.flow(ProjectProperties.producerTransactionSettings, "producer"))

    .runWith(Sink.ignore)


  /** CONSUMER **/
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
      println(f"      Receive <- productId: ${idRecord}%-3s| ${productName}%-8s| amount: ${amount}%-2s| price: ${price}%-6s")
      val y = x.drop(2).map((y) => y.toDouble)
      y(0) * y(1)
    })
    .map((x) => {
      finalPrice += BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      // println(f"    Receive <- productId: ${idRecord}%-3s| ${productName}%-8s| amount: ${amount}%-2s| price: ${price}%-6s")
      if (idRecord == 30) {
        println(s"\n FINAL PRICE: $finalPrice")
      }
    })

  //      .runWith(Sink.foreach((x) => {
  //        finalPrice += BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  //        // println(f"    Receive <- productId: ${idRecord}%-3s| ${productName}%-8s| amount: ${amount}%-2s| price: ${price}%-6s")
  //        if (idRecord == 30) {
  //          println(s"\n FINAL PRICE: $finalPrice")
  //        }
  //      }))

  val singleSource = Source.single(1)
  val singleSink = Sink.ignore
  // val exampleFlow = Flow[Int].filter(_>100)


  val stream = RestartSource.onFailuresWithBackoff(
    minBackoff = 1.seconds,
    maxBackoff = 10.seconds,
    randomFactor = 0.2,
    maxRestarts = 2
  ) {

    /** GRAPH */
    val graph = Source.fromGraph(GraphDSL.create() {
      implicit builder =>
        import GraphDSL.Implicits._

        val broadcast = builder.add(Broadcast[Int](3))

        //        broadcast.out(0) ~> sourceProducer ~> singleSink
        //        broadcast.out(1) ~> transaction ~> singleSink
        //        broadcast.out(2) ~> consumer ~> singleSink

        ClosedShape
    })
      .run()

  }


  /** SinkFlow **/
  val sinkFlow = Flow.fromGraph(GraphDSL.create() {implicit builder =>

  val sourceShape = builder.add(source)
  val sinkShape = builder.add(sink)

  })

  val source: Source[Int, NotUsed] =
    Source.fromIterator(() => Iterator.continually(ThreadLocalRandom.current().nextInt(100))).take(100)

  val countSink: Sink[Int, Future[Int]] = Flow[Int].toMat(Sink.fold(0)((acc, elem) => acc + 1))(Keep.right)
  val minSink: Sink[Int, Future[Int]] = Flow[Int].toMat(Sink.fold(0)((acc, elem) => math.min(acc, elem)))(Keep.right)
  val maxSink: Sink[Int, Future[Int]] = Flow[Int].toMat(Sink.fold(0)((acc, elem) => math.max(acc, elem)))(Keep.right)

  val (count: Future[Int], min: Future[Int], max: Future[Int]) =
    RunnableGraph
      .fromGraph(GraphDSL.create(countSink, minSink, maxSink)(Tuple3.apply) {
        implicit builder =>
          (countS, minS, maxS) =>
            import GraphDSL.Implicits._
            val broadcast = builder.add(Broadcast[Int](3))
            source ~> broadcast
            broadcast.out(0) ~> countS
            broadcast.out(0) ~> minS
            broadcast.out(0) ~> maxS
            ClosedShape
      })
      .run()


}
