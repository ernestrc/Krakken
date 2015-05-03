package krakken.utils

import akka.event.LoggingAdapter
import com.mongodb.DBObject
import com.mongodb.casbah.commons.MongoDBObject
import com.novus.salat.Grater
import krakken.model.Receipt.Empty
import krakken.model._
import org.bson.types.ObjectId
import spray.http._
import spray.httpx.marshalling.Marshaller
import spray.httpx.unmarshalling.{Deserializer, FromRequestUnmarshaller, Unmarshaller}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

object Implicits {

  implicit class stringPath(root: String){
    def /(path:String):String = root + "/" + path
  }

  implicit class pimpedObjectId(_id: ObjectId){
    def toSid:SID = _id.toString
  }

  implicit class pimpedAny[T](any: T)(implicit log: LoggingAdapter){
    def Ω(message:T ⇒ String) = {
      log.debug(message(any))
      any
    }
  }

  implicit class pimpedFromHintGrater(a: PartialFunction[TypeHint, Grater[_]]){
    def ~(b: PartialFunction[TypeHint, Grater[_]]):PartialFunction[TypeHint, Grater[_]] = a.orElse(b)
  }

  implicit class pimpedFutureOfReceipt(f: Future[Any]){
    def >>>[T <: AnyRef](implicit ex: ExecutionContext, format: JsonFormat[T], log: LoggingAdapter) : Future[JsObject] = {
      f.map{ receipt ⇒
        try receipt.asInstanceOf[Receipt[T]].json
        catch {
          case e: Exception ⇒
            log.debug("PimpedFutureOfReceipt >>> method try catched and.. {}", e.toString)
            receipt.asInstanceOf[Receipt[Empty]].json
        }
      }
    }
  }

  private [krakken] def deserialize(value:JsValue):Any = {
    value match {
      case JsBoolean(x) ⇒ x
      case JsString(x) ⇒ x
      case JsNumber(x) ⇒ x.toLong
      case JsNull ⇒ None
      case JsArray(x) ⇒ x.foldLeft(List.empty[Any]){ (acc, y) ⇒
        (y match {
          case js: JsObject ⇒ jsonToBson(js)
          case lse ⇒ deserialize(lse)
        }) :: acc }
      case ob @ JsObject(x) ⇒ jsonToBson(ob)
    }
  }

  implicit def jsonToBson(json: JsObject): DBObject = {
    MongoDBObject(json.fields.map(kv ⇒ kv._1 → deserialize(kv._2)).toList)
  }

  implicit class pimpedSID(id: SID){
    def toObjectId:ObjectId = new ObjectId(id.toString)
  }

  implicit def graterUnmarshallerConverter[T <: AnyRef](grater: Grater[T]): Unmarshaller[T] =
    graterUnmarshaller(grater)

  def graterUnmarshaller[T <: AnyRef](grater: Grater[T]) =
    Unmarshaller[T](MediaTypes.`application/json`) {
      case x: HttpEntity.NonEmpty ⇒
        grater.fromJSON(x.asString(defaultCharset = HttpCharsets.`UTF-8`))
    }

  implicit def graterFromResponseUnmarshaller[T <: AnyRef](grater: Grater[T])
  : FromRequestUnmarshaller[T] = Deserializer.fromFunction2Converter{ req: HttpRequest ⇒
    grater.fromJSON(req.entity.asString)
  }

  implicit def graterMarshallerConverter[T <: AnyRef](grater: Grater[T]): Marshaller[T] =
    graterMarshaller[T](grater)

  def graterMarshaller[T <: AnyRef](grater: Grater[T], pretty: Boolean = false) =
    Marshaller.delegate[T, String](ContentTypes.`application/json`) { value ⇒
      if(!pretty) grater.toPrettyJSON(value)
      else grater.toCompactJSON(value)
    }

  implicit class StringCanHint(s:String){
    def toHint: TypeHint = InjectedTypeHint(s)
  }
}
