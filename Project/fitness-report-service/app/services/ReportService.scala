package services

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import javax.inject.{Inject, Singleton}
import models.Report
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import play.api.libs.json.Json
import repository.ReportRepository
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class ReportService @Inject()(reportRepository: ReportRepository)
                             (implicit ec: ExecutionContext, system: ActorSystem, mat: Materializer) {

  private val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers("34.125.145.88:9092")
    .withGroupId("report1-group")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  Consumer
    .plainSource(consumerSettings, Subscriptions.topics("workout1-topic"))
    .mapAsync(1) { msg =>
      println("COSUMED MESSAGE "+ msg)
//      processWorkoutData(msg.value())
      val json = Json.parse(msg.value())
      println("THE JSON IS " + json);
      val report = json.as[Report]
      println("THE REPORT IN JSON IS " + report)
      reportRepository.createReport(report).map(_ => ())
    }
    .runWith(Sink.ignore)

//  def processWorkoutData(data: String): Future[Unit] = {
//    val json = Json.parse(data)
//    val report = json.as[Report]
//    println("THE REPORT IN JSON IS " + report)
//    reportRepository.createReport(report).map(_ => ())
//  }

  def getReports(userId: Long): Future[Seq[Report]] = {
    reportRepository.getReportsByUserId(userId)
  }
}
