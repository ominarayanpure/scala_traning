package repository

import akka.kafka.ProducerSettings
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import javax.inject.{Inject, Singleton}
import models.Workout
import play.api.db.slick.DatabaseConfigProvider
import slick.jdbc.MySQLProfile
import akka.actor.ActorSystem
import akka.stream.Materializer
import slick.jdbc.MySQLProfile.api._
import play.api.libs.json.Json
import akka.stream.scaladsl.Source
import akka.kafka.scaladsl.Producer
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class WorkoutRepository @Inject()(protected val dbConfigProvider: DatabaseConfigProvider)
                                 (implicit ec: ExecutionContext, system: ActorSystem, mat: Materializer) {
  private val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers("34.16.208.7:9092")

  val dbConfig = dbConfigProvider.get[MySQLProfile]
  import dbConfig._
  private val workouts = TableQuery[WorkoutsTable]

  def createWorkout(workout: Workout): Future[Workout] = {
    val insertQuery = workouts returning workouts.map(_.id) into ((workout, id) => workout.copy(id = Some(id)))

    // Appending in Kafka
    val message = Json.obj(
      "user_id" -> workout.userId,
      "calories_burned" -> workout.caloriesBurned,
      "duration" -> workout.duration,
      "exercise_type" -> workout.exercise,
      "date" -> workout.date
    ).toString()

    val producerRecord = new ProducerRecord[String, String]("workout1-topic", workout.userId.toString, message)
    Source.single(producerRecord)
      .runWith(Producer.plainSink(producerSettings))
      .map(_ => ())

    db.run(insertQuery += workout)
  }

  def getWorkoutsByUserId(userId: Long): Future[Seq[Workout]] = {
    db.run(workouts.filter(_.userId === userId).result)
  }

  def getAllWorkouts: Future[Seq[Workout]] = {
    db.run(workouts.result)
  }

  private class WorkoutsTable(tag: Tag) extends Table[Workout](tag, "workouts") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def userId = column[Long]("user_id")
    def exercise = column[String]("exercise")
    def duration = column[Int]("duration")
    def caloriesBurned = column[Int]("calories_burned")
    def date = column[java.sql.Date]("date")

    def * = (id.?, userId, exercise, duration, caloriesBurned, date) <> ((Workout.apply _).tupled, Workout.unapply)
  }
}
