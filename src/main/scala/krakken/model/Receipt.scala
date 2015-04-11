package krakken.model

import com.novus.salat.global._
import com.novus.salat.{Grater, _}

/**
 * Basic command-side reporting class.
 *
 * @param success Quick check if task was successful
 * @param errors List of errors, by default empty.
 */
case class Receipt(success: Boolean, updated: SID = "",
                   message: String = "", errors: List[String] = List.empty)

object Receipt {

  def error(e: Throwable, message: String = ""): Receipt =
    Receipt(success = false, message = message, errors = List(e.getMessage))

  def compileResults(s: Seq[Receipt]): Receipt =
    s.foldLeft(Receipt(success = true)) { (globalRes, taskRes) =>
      Receipt(globalRes.success && taskRes.success, errors = taskRes.errors ::: globalRes.errors,
        message = globalRes.message + "-" + taskRes.message)
    }

  implicit val receiptGrater: Grater[Receipt] = grater[Receipt]

}
