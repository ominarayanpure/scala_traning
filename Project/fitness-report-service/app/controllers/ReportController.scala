package controllers

import javax.inject._
import play.api.mvc._
import play.api.libs.json._
import services.ReportService
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class ReportController @Inject()(cc: ControllerComponents, reportService: ReportService)
                                (implicit ec: ExecutionContext) extends AbstractController(cc) {


  def viewReports(userId: Long): Action[AnyContent] = Action.async {
    reportService.getReports(userId).map { reports =>
      Ok(views.html.reports(reports))
    }
  }
}
