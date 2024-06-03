package services

import javax.inject.{Inject, Singleton}
import models.Workout
import repository.WorkoutRepository
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class WorkoutService @Inject()(workoutRepository: WorkoutRepository)(implicit ec: ExecutionContext) {

  def logWorkout(workout: Workout): Future[Workout] = {
    workoutRepository.createWorkout(workout)
  }

  def getWorkouts(userId: Long): Future[Seq[Workout]] = {
    workoutRepository.getWorkoutsByUserId(userId)
  }

  def getAllWorkouts: Future[Seq[Workout]] = {
    workoutRepository.getAllWorkouts
  }
}
