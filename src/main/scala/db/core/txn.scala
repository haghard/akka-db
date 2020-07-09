package db.core

import akka.event.LoggingAdapter
import org.rocksdb.{RocksDBException, Transaction}

import scala.util.control.{NoStackTrace, NonFatal}

object txn {

  final case class InvariantViolation(msg: String) extends Exception(msg) with NoStackTrace

  def withTxn[T <: Transaction](txn: T, logger: LoggingAdapter)(
    f: T ⇒ String
  ): Either[Throwable, String] = {

    def txnErrorHandler(
      txn: Transaction,
      logger: LoggingAdapter
    ): PartialFunction[Throwable, Either[Throwable, String]] = {
      //concurrent modification
      case ex: RocksDBException ⇒
        val errName = ex.getStatus.getCode.name
        logger.error(s"Transaction error: $errName", ex)
        txn.rollback
        Left(new Exception(s"Transaction error: $errName"))
      case NonFatal(ex) ⇒
        logger.error(s"Transaction error", ex)
        txn.rollback
        Left(ex)
    }

    def write[T <: Transaction](txn: T)(
      f: T ⇒ Either[Throwable, String]
    )(onError: PartialFunction[Throwable, Either[Throwable, String]]): Either[Throwable, String] =
      try {
        val r = f(txn)
        txn.commit()
        r
      } catch onError
      finally txn.close

    //write[T](txn)(f)(txnErrorHandler(txn, logger))

    try {
      val key = f(txn)
      txn.commit()
      Right(key)
    } catch {
      //concurrent modification
      case ex: RocksDBException ⇒
        val errName = ex.getStatus.getCode.name
        logger.error(s"Transaction error: $errName", ex)
        txn.rollback
        Left(new Exception(s"Transaction error: $errName"))
      case NonFatal(ex) ⇒
        logger.error(s"Transaction error", ex)
        txn.rollback
        Left(ex)
    } finally txn.close
  }

  def readTxn[T <: Transaction](txn: T, logger: LoggingAdapter)(
    f: T ⇒ Either[Throwable, Option[Set[String]]]
  ): Either[Throwable, Option[Set[String]]] =
    try {
      val r = f(txn)
      txn.commit
      r
    } catch {
      case NonFatal(ex) ⇒
        logger.error("Transaction [Read] error", ex)
        txn.rollback
        Left(ex)
    } finally txn.close

  def readTxn0[T <: Transaction](txn: T, logger: LoggingAdapter)(
    f: T ⇒ Option[String]
  ): Either[Throwable, Option[String]] =
    try {
      val r = f(txn)
      txn.commit()
      Right(r)
    } catch {
      case NonFatal(ex) ⇒
        logger.error("Transaction [Read] error", ex)
        txn.rollback
        Left(ex)
    } finally txn.close
}
