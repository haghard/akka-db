package db.core

import akka.event.LoggingAdapter
import org.rocksdb.{ RocksDBException, Transaction }

import scala.util.control.{ NoStackTrace, NonFatal }

object rocks {

  case class InvariantViolation(msg: String) extends Exception(msg) with NoStackTrace

  private def txnErrorHandler(txn: Transaction, logger: LoggingAdapter): PartialFunction[Throwable, Either[Throwable, String]] = {
    //concurrent modification
    case ex: RocksDBException ⇒
      logger.error(s"Transact error: ${ex.getStatus.getCode.name}")
      txn.rollback()
      Left(new Exception(s"Transact error: ${ex.getStatus.getCode.name}"))
    case NonFatal(ex) ⇒
      logger.error(s"Transact error: ${ex.getMessage}")
      txn.rollback()
      Left(ex)
  }

  private def managedR[T <: Transaction](txn: T, logger: LoggingAdapter)(f: T ⇒ Either[Throwable, String])(
    onError: PartialFunction[Throwable, Either[Throwable, String]]): Either[Throwable, String] =
    try {
      val r = f(txn)
      txn.commit()
      r
    } catch onError
    finally txn.close()

  def writeTxn[T <: Transaction](txn: T, logger: LoggingAdapter)(f: T ⇒ Either[Throwable, String]): Either[Throwable, String] =
    managedR[T](txn, logger)(f)(txnErrorHandler(txn, logger))

  def readTxn[T <: Transaction](txn: T, logger: LoggingAdapter)(f: T ⇒ Either[Throwable, Option[Set[String]]]): Either[Throwable, Option[Set[String]]] =
    try {
      val r = f(txn)
      txn.commit()
      r
    } catch {
      case NonFatal(ex) ⇒
        logger.error("transactGet error: " + ex.getMessage)
        txn.rollback()
        Left(ex)
    } finally txn.close()

  def readTxn0[T <: Transaction](txn: T, logger: LoggingAdapter)(f: T ⇒ Either[Throwable, Option[String]]): Either[Throwable, Option[String]] =
    try {
      val r = f(txn)
      txn.commit()
      r
    } catch {
      case NonFatal(ex) ⇒
        logger.error("transactGet error: " + ex.getMessage)
        txn.rollback()
        Left(ex)
    } finally txn.close()
}
