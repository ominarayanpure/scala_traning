package models

import play.api.libs.json._
import java.sql.Date

case class Workout(id: Option[Long], userId: Long, exercise: String, duration: Int, caloriesBurned: Int, date: Date)

object Workout {
  implicit val workoutFormat: Format[Workout] = Json.format[Workout]
}