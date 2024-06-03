package models

import play.api.libs.json._
import java.sql.Date

case class Report(
                   id: Option[Long],
                   user_id: Long,
                   exercise_type: String,
                   duration: Int,
                   calories_burned: Int,
                   date: Date
                 )

object Report {
  implicit val reportFormat: OFormat[Report] = Json.format[Report]
}
