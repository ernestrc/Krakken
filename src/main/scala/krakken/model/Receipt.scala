package krakken.model

import com.novus.salat.global._
import com.novus.salat.{Grater, _}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsonFormat, RootJsonFormat}

/**
 * Basic command-side reporting class.
 *
 * @param success Task was successful
 * @param errors List of errors, by default empty.
 */
case class Receipt[T](success: Boolean, entity: Option[T],
                   message: String = "", errors: List[String] = List.empty)

object Receipt {

  def error(e: Throwable, message: String = ""): Receipt[Nothing] =
    Receipt[Nothing](success = false, None, message, List(e.getMessage))

  def compileResults(s: Seq[Receipt[_]]): Receipt[_] = {
    var result: Receipt[_] = if (s.nonEmpty) null else Receipt[Nothing](success = false, None)
    var receipts = s
    while(receipts.nonEmpty){
      result = Receipt(
        result.success && receipts.head.success,
        errors = receipts.head.errors ::: result.errors,
        message = result.message + "-" + receipts.head.message,
        entity = receipts.head.entity)
      receipts = receipts.tail
    }
    result
  }

  def receiptGrater[T : Manifest]: Grater[Receipt[T]] = grater[Receipt[T]]
  def receiptFormat[T : JsonFormat]: RootJsonFormat[Receipt[T]] = jsonFormat4(Receipt.apply)

}
