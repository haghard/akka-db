package db.core

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import akka.actor.typed.ActorRef
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.adapter._
import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.Cluster
import akka.event.LoggingAdapter
import akka.pattern.pipe
import db.core.MVCCStorageBackend.{ReservationReply, Reserve, _}
import org.rocksdb.{Options, _}
import org.rocksdb.util.SizeUnit

import scala.concurrent.Future
import scala.util.Try
import scala.util.control.NonFatal

object MVCCStorageBackend {
  val path       = "rocks-db"
  val ticketsNum = 200

  val Key = ServiceKey[MVCCStorageBackend.Protocol]("StorageBackend")

  sealed trait Protocol

  final case class Reserve(voucher: String, client: String, replyTo: ActorRef[ReservationReply]) extends Protocol

  sealed trait ReservationReply {
    def key: String
    def replyTo: ActorRef[ReservationReply]
  }

  object ReservationReply {
    final case class Success(key: String, replyTo: ActorRef[ReservationReply])                 extends ReservationReply
    final case class Closed(key: String, replyTo: ActorRef[ReservationReply])                  extends ReservationReply
    final case class Failure(key: String, err: Throwable, replyTo: ActorRef[ReservationReply]) extends ReservationReply
  }

  def managedIter(r: RocksIterator, log: LoggingAdapter)(f: RocksIterator ⇒ Unit) =
    try f(r)
    catch {
      case NonFatal(ex) ⇒ log.error(ex, "RocksIterator error:")
    } finally r.close

  def props(receptionist: ActorRef[Receptionist.Command]) =
    Props(new MVCCStorageBackend(receptionist)).withDispatcher("akka.db-io")
}

/*

https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/TransactionSample.java
https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/OptimisticTransactionSample.java
https://github.com/facebook/rocksdb/wiki/Transactions
https://github.com/facebook/rocksdb/wiki/Merge-Operator
https://github.com/facebook/rocksdb/blob/a283800616cb5da5da43d878037e6398cccf9090/java/src/test/java/org/rocksdb/RocksDBTest.java

Example: Sell N tickets without overselling

SNAPSHOT ISOLATION (Can't be totally available)
https://jepsen.io/consistency/models/snapshot-isolation

  When a txn starts, it sees a consistent snapshot of the db that existed at the moment that the txn started.
  If two txns update the same object, then first writer wins.
  We get SI automatically for free with MVCC

Main benefits of MVCC
 * Writers don't block readers
 * Read-only txns can read a shapshot without acquiring a lock.

 Allows WRITE SKEW anomaly in general. However, it's impossible
  to run into WRITE SKEW in this example because in order to get it our transaction should touch two or more keys
 */

//https://github.com/facebook/rocksdb/tree/master/java/src/main/java/org/rocksdb
final class MVCCStorageBackend(receptionist: ActorRef[Receptionist.Command]) extends Actor with ActorLogging {
  org.rocksdb.RocksDB.loadLibrary()

  receptionist ! akka.actor.typed.receptionist.Receptionist.Register(MVCCStorageBackend.Key, self)

  Try(Files.createDirectory(Paths.get(s"./$path")))

  val cluster = Cluster(context.system)
  val sa      = cluster.selfAddress

  implicit val ec = context.system.dispatchers.lookup("akka.db-io")

  val SEPARATOR = '@'
  val dbPath    = new File(s"./$path/replica-${cluster.selfAddress.port.get}").getAbsolutePath

  val options = new Options()
    .setCreateIfMissing(true)
    .setWriteBufferSize(10 * SizeUnit.KB)
    .setMaxWriteBufferNumber(3)
    .setMaxSubcompactions(10)
    .setMaxBackgroundJobs(3)
    .setMergeOperator(new org.rocksdb.StringAppendOperator(SEPARATOR)) //new CassandraValueMergeOperator() doesn't work
    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
    .setCompactionStyle(CompactionStyle.UNIVERSAL)

  val txnDbOptions = new TransactionDBOptions()
  val writeOptions = new WriteOptions()

  val txnDb: TransactionDB =
    TransactionDB.open(options, txnDbOptions, dbPath)

  override def preStart(): Unit = {
    log.info("dbPath:{}", dbPath)
    MVCCStorageBackend.managedIter(txnDb.newIterator(new ReadOptions()), log) { iter ⇒
      iter.seekToFirst
      while (iter.isValid) {
        val key   = new String(iter.key, UTF_8)
        val sales = new String(iter.value, UTF_8).split(SEPARATOR)
        log.info("{} [{}:{}]", cluster.selfAddress.port.get, key, sales.size)
        iter.next
      }
    }
  }

  override def postStop(): Unit = {
    log.warning("Stop db node {}", dbPath)
    txnDb.close()
  }

  def put(key: String, value: String, replyTo: ActorRef[ReservationReply]): ReservationReply =
    txn
      .withTxn(txnDb.beginTransaction(writeOptions, new TransactionOptions().setSetSnapshot(true)), log) { txn ⇒
        //Guards against Read-Write Conflicts:
        // txn.getForUpdate ensures that no other writer modifies any keys that were read by this transaction.
        val snapshot = txn.getSnapshot
        val keyBytes = key.getBytes(UTF_8)

        val salesBts = txn.getForUpdate(new ReadOptions().setSnapshot(snapshot), keyBytes, true)
        val sales    = Try(new String(salesBts, UTF_8).split(SEPARATOR)).getOrElse(Array.empty[String])

        if (sales.size < ticketsNum) {
          //use merge to activate org.rocksdb.StringAppendOperator
          txn.merge(key.getBytes(UTF_8), value.getBytes(UTF_8))
          Some(key)
        } else None
        //TODO: This isn't InvariantViolation. Use smth else to signal the end of the sells
        //throw db.core.txn.InvariantViolation(s"Key $key. All tickets have been sold")
      }
      .fold(
        ReservationReply.Failure(key, _, replyTo),
        _.fold[ReservationReply](ReservationReply.Closed(key, replyTo))(ReservationReply.Success(_, replyTo))
      )

  def write: Receive = {
    case Reserve(key, value, replyTo) ⇒
      Future(put(key, value, replyTo)).mapTo[ReservationReply].pipeTo(self)
    case r: ReservationReply ⇒
      r.replyTo.tell(r)
    /*
      r match {
        case reply: ReservationReply.Success ⇒
          reply.replyTo.tell(reply)
        case reply: ReservationReply.Failure ⇒
          reply.replyTo ! reply
        case reply: ReservationReply.Closed =>
          reply.replyTo ! reply
          Behaviors.stopped
      }*/
  }

  override def receive: Receive =
    write /*orElse read*/ orElse {
      case scala.util.Failure(ex) ⇒
        log.error(ex, "Unexpected error")
    }
}
