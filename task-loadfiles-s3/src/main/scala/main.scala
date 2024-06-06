import JsonFormats._
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.stream.scaladsl.StreamConverters
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.{AmazonClientException, AmazonServiceException}
import com.typesafe.config.ConfigFactory

import java.io.{File, FileOutputStream}
import scala.concurrent.Future

object main {
  val config = ConfigFactory.load()
  private val awsAccessKeyId = config.getString("aws.access-key-id")
  private val awsSecretAccessKey = config.getString("aws.secret-access-key")
  private val bucketName = config.getString("aws.s3.bucket-name")
  val FILE_NAME = "newTextFile" //  key-name

  implicit val system = ActorSystem(Behaviors.empty, "MyActorSystem")

  def main(args: Array[String]): Unit = {
    val awsCredentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey)
    val amazonS3Client = new AmazonS3Client(awsCredentials)
    val route = concat(
      post {
        path("save-file") {
          entity(as[FileRequest]) { fileRequest =>
            // create a new bucket - already created in s3
            // otherwise use - amazonS3Client.createBucket(BUCKET_NAME)
            try {
              val file = new File(fileRequest.filePath)
              amazonS3Client.putObject(bucketName, FILE_NAME, file)
            }
            catch {
              case ase: AmazonServiceException => System.err.println("Exception: " + ase.toString)
              case ace: AmazonClientException => System.err.println("Exception: " + ace.toString)
            }
            complete(StatusCodes.OK, "File Uploaded to S3")
          }
        }
      },
      get {
        path("get-file") {
          val obj = amazonS3Client.getObject(bucketName, FILE_NAME)

          val source = StreamConverters.fromInputStream(() => obj.getObjectContent)

          val byteArrayFuture: Future[Array[Byte]] = source.runFold(Array.emptyByteArray) { (acc, bytes) =>
            acc ++ bytes
          }

          onComplete(byteArrayFuture) {
            case scala.util.Success(bytes) =>
              val file = new FileOutputStream(FILE_NAME)
              file.write(bytes)
              file.close()
              complete(StatusCodes.OK, "File downloaded and written locally from S3")

            case scala.util.Failure(ex) =>
              complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
          }
        }
      })
    Http().newServerAt("0.0.0.0", 6080).bind(route)
    println("Server online at http://0.0.0.0:6080/")
  }
}