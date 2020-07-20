package NoTransaction

object Main extends App {

  val producer: Producer = new Producer
  val producerThread = new Thread(producer)

  val consumer: Consumer = new Consumer
  val consumerThread = new Thread(consumer)

  producerThread.start()
  consumerThread.start()
}
