package controllers

import javax.inject._
import play.api.mvc._
import play.api.libs.json._
import services.WorkoutService
import models.Workout
import scala.concurrent.{ExecutionContext, Future}
import java.sql.Date

@Singleton
class WorkoutController @Inject()(cc: ControllerComponents, workoutService: WorkoutService)(implicit ec: ExecutionContext) extends AbstractController(cc) {

  def index: Action[AnyContent] = Action { implicit request =>
    Ok(views.html.index(request))
  }

  def logWorkout: Action[AnyContent] = Action.async { implicit request =>
    val formData = request.body.asFormUrlEncoded

    val userId = formData.flatMap(_.get("userId").flatMap(_.headOption)).map(_.toLong)
    val exercise = formData.flatMap(_.get("exercise").flatMap(_.headOption))
    val duration = formData.flatMap(_.get("duration").flatMap(_.headOption)).map(_.toInt)
    val caloriesBurned = formData.flatMap(_.get("caloriesBurned").flatMap(_.headOption)).map(_.toInt)
    val date = formData.flatMap(_.get("date").flatMap(_.headOption)).map(Date.valueOf)

    (userId, exercise, duration, caloriesBurned, date) match {
      case (Some(userId), Some(exercise), Some(duration), Some(caloriesBurned), Some(date)) =>
        val workout = Workout(None, userId, exercise, duration, caloriesBurned, date)
        workoutService.logWorkout(workout).map { _ =>
          Redirect(routes.WorkoutController.index)
        }
      case _ =>
        Future.successful(BadRequest("Invalid form data"))
    }
  }

  def listWorkouts(userId: Long): Action[AnyContent] = Action.async { implicit request =>
    workoutService.getWorkouts(userId).map { workouts =>
      Ok(views.html.workoutsList(workouts))
    }
  }

  def listAllWorkouts: Action[AnyContent] = Action.async { implicit request =>
    workoutService.getAllWorkouts.map { workouts =>
      Ok(views.html.allWorkouts(workouts))
    }
  }

}
