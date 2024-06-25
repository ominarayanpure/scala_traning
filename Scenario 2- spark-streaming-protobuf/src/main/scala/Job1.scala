//package omkar

import omkar.{KafkaService, MetricGenerator}

import scala.concurrent.duration._
import scala.io.Source

object Job1 {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem[Nothing](Behaviors.empty, "ServerMetricsSystem")
    implicit val ec = system.executionContext
    implicit val materializer: Materializer = Materializer(system)

    val kafkaProducerService = new KafkaService("localhost:9092", "server-metrics")

    Source.tick(0.seconds, 5.seconds, NotUsed)
      .map(_ => MetricGenerator.generateMetric())
      .runWith(Sink.foreach(metric => kafkaProducerService.send(metric)))

  }
}

