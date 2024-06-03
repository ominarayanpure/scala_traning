package repository

import javax.inject.{Inject, Singleton}
import models.Report
import play.api.db.slick.DatabaseConfigProvider
import slick.jdbc.MySQLProfile
import slick.jdbc.MySQLProfile.api._
import scala.concurrent.{ExecutionContext, Future}
import java.sql.Date

@Singleton
class ReportRepository @Inject()(protected val dbConfigProvider: DatabaseConfigProvider)
                                (implicit ec: ExecutionContext) {

  val dbConfig = dbConfigProvider.get[MySQLProfile]
  import dbConfig._

  private val reports = TableQuery[ReportsTable]

  def createReport(report: Report): Future[Report] = {
    val insertQuery = reports returning reports.map(_.id) into ((report, id) => report.copy(id = Some(id)))
    db.run(insertQuery += report)
  }

  def getReportsByUserId(userId: Long): Future[Seq[Report]] = {
    db.run(reports.filter(_.userId === userId).result)
  }

  private class ReportsTable(tag: Tag) extends Table[Report](tag, "reports") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def userId = column[Long]("user_id")
    def exercise = column[String]("exercise_type")
    def duration = column[Int]("duration")
    def caloriesBurned = column[Int]("calories_burned")
    def date = column[Date]("date")

    def * = (id.?, userId, exercise, duration, caloriesBurned, date) <> ((Report.apply _).tupled, Report.unapply)
  }
}
