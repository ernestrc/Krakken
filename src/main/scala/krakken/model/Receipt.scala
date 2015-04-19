package krakken.model

import com.novus.salat._
import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, JsonFormat, RootJsonFormat}

/**
 * Basic command-side reporting class.
 *
 * @param success Task was successful
 * @param errors List of errors, by default empty.
 */
case class Receipt[T](success: Boolean, entity: T,
                      message: String = "", errors: List[String] = List.empty) {

  def json(implicit graterT: JsonFormat[T]): JsObject = {
    Receipt.receiptJsonFormat[T].write(this).asJsObject
  }
}

object Receipt {

  def receiptJsonFormat[T: JsonFormat]: RootJsonFormat[Receipt[T]] =
    jsonFormat4(Receipt.apply)

  case class Empty()

  object Empty {
    implicit val jsonFormat: RootJsonFormat[Empty] = jsonFormat0(Empty.apply)
  }

  def error(e: Throwable, message: String = ""): Receipt[Empty] =
    Receipt(success = false, message = message, errors = List(e.getMessage),
      entity = Empty())

  def compileResults(s: Seq[Receipt[_]]): Receipt[_] = {
    var result: Receipt[_] = if (s.nonEmpty) null
    else Receipt(success = false,
      entity = Empty())
    var receipts = s
    while (receipts.nonEmpty) {
      result = Receipt(
        result.success && receipts.head.success,
        errors = receipts.head.errors ::: result.errors,
        message = result.message + "-" + receipts.head.message,
        entity = receipts.head.entity)
      receipts = receipts.tail
    }
    result
  }

  def receiptGrater[T: Manifest]: Grater[Receipt[T]] = grater[Receipt[T]]

}
