import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

case class FileRequest(filePath: String)

object JsonFormats {
  implicit val fileRequestFormat: RootJsonFormat[FileRequest] = jsonFormat1(FileRequest)
}