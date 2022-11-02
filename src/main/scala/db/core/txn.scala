package db.core

import akka.event.LoggingAdapter
import org.rocksdb.{RocksDBException, Transaction}

import scala.util.control.{NoStackTrace, NonFatal}

/*

Resource management: https://medium.com/@bszwej/composable-resource-management-in-scala-ce902bda48b2
Loan pattern
  def withSqsConsumer[T](resource: SqsConsumer)(handle: SqsConsumer => T): T =
    try handle(resource) finally resource.close()

  withSqsConsumer(new SqsConsumer{}) { consumer: SqsConsumer =>
      consumer.poll
  }

  def withResource[R, T](resource: => R)(handle: R => T)(close: R => Unit): T =
    try handle(resource) finally close(resource)
 */
object txn {

  final case class DBError(cause: RocksDBException) extends Exception(cause) with NoStackTrace

  def withTxn[T <: Transaction](txn: T, logger: LoggingAdapter)(
    handle: T => Option[String]
  ): Either[Throwable, Option[String]] =
    /*
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

    write[T](txn)(f)(txnErrorHandler(txn, logger))
     */

    try {
      val key = handle(txn)
      txn.commit()
      Right(key)
    } catch {
      // concurrent modification
      case ex: RocksDBException =>
        val errName = ex.getStatus.getCode.name
        logger.error(s"Transaction error: $errName", ex)
        txn.rollback
        Left(DBError(ex))
      case NonFatal(ex) =>
        logger.error(s"Transaction error", ex)
        txn.rollback
        Left(ex)
    } finally txn.close

  def readTxn[T <: Transaction](txn: T, logger: LoggingAdapter)(
    f: T => Either[Throwable, Option[Set[String]]]
  ): Either[Throwable, Option[Set[String]]] =
    try {
      val r = f(txn)
      txn.commit
      r
    } catch {
      case NonFatal(ex) =>
        logger.error("Transaction [Read] error", ex)
        txn.rollback
        Left(ex)
    } finally txn.close

  def readTxn0[T <: Transaction](txn: T, logger: LoggingAdapter)(
    f: T => Option[String]
  ): Either[Throwable, Option[String]] =
    try {
      val r = f(txn)
      txn.commit()
      Right(r)
    } catch {
      case NonFatal(ex) =>
        logger.error("Transaction [Read] error", ex)
        txn.rollback
        Left(ex)
    } finally txn.close
}
