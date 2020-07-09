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
import db.core.DB._
import db.core.MVCCStorageBackend.{ReservationReply, ReserveSeat, _}
import org.rocksdb.{Options, _}
import org.rocksdb.util.SizeUnit

import scala.concurrent.Future
import scala.util.Try
import scala.util.control.NonFatal

object MVCCStorageBackend {

  sealed trait Protocol

  final case class ReserveSeat(voucher: String, client: String, replyTo: ActorRef[ReservationReply]) extends Protocol

  sealed trait ReservationReply {
    def key: String
  }

  object ReservationReply {

    final case class Success(key: String, replyTo: ActorRef[ReservationReply]) extends ReservationReply

    final case class Failure(key: String, th: Throwable, replyTo: ActorRef[ReservationReply]) extends ReservationReply

  }

  val sbKey = ServiceKey[KVProtocol]("StorageBackend")

  val sep        = ','
  val path       = "rocks-db"
  val ticketsNum = 200

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
class MVCCStorageBackend(receptionist: akka.actor.typed.ActorRef[Receptionist.Command])
    extends Actor
    with ActorLogging {
  org.rocksdb.RocksDB.loadLibrary()

  receptionist ! akka.actor.typed.receptionist.Receptionist.Register(KeyValueStorageBackend.serviceKey, self)

  Try(Files.createDirectory(Paths.get(s"./$path")))

  val cluster = Cluster(context.system)
  val sa      = cluster.selfAddress

  implicit val ec = context.system.dispatchers.lookup("akka.db-io")

  val dbPath = new File(s"./$path/replica-${cluster.selfAddress.port.get}").getAbsolutePath

  val options = new Options()
    .setCreateIfMissing(true)
    .setWriteBufferSize(10 * SizeUnit.KB)
    .setMaxWriteBufferNumber(3)
    .setMaxBackgroundCompactions(10)
    .setMergeOperator(new org.rocksdb.StringAppendOperator(',')) //new CassandraValueMergeOperator() doesn't work
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
        val sales = new String(iter.value, UTF_8).split(sep)
        log.info("{} [{}:{}]", cluster.selfAddress.port.get, key, sales.size)
        iter.next
      }
    }
  }

  override def postStop(): Unit = {
    log.warning("Stop db node {}", dbPath)
    txnDb.close()
  }

  def put(key: String, value: String, replyTo: akka.actor.typed.ActorRef[ReservationReply]): ReservationReply =
    txn
      .mvccWrite(txnDb.beginTransaction(writeOptions, new TransactionOptions().setSetSnapshot(true)), log) { txn ⇒
        val kb = key.getBytes(UTF_8)

        /*
        Guards against Read-Write Conflicts:
          txn.getForUpdate ensures that no other writer modifies any keys that were read by this transaction.
         */

        val snapshot = txn.getSnapshot
        val salesBts = txn.getForUpdate(new ReadOptions().setSnapshot(snapshot), kb, true)
        val sales    = Try(new String(salesBts, UTF_8).split(sep)).getOrElse(Array.empty[String])

        //use merge to activate org.rocksdb.StringAppendOperator
        if (sales.size < ticketsNum) txn.merge(key.getBytes(UTF_8), value.getBytes(UTF_8))
        else throw db.core.txn.InvariantViolation(s"Key $key. All tickets have been sold")
        Right(key)
      }
      .fold(ReservationReply.Failure(key, _, replyTo), ReservationReply.Success(_, replyTo))

  def write: Receive = {
    case ReserveSeat(key, value, replyTo) ⇒
      Future(put(key, value, replyTo)).mapTo[ReservationReply].pipeTo(self)
    case r: ReservationReply ⇒
      r match {
        case reply: ReservationReply.Success ⇒
          reply.replyTo.tell(reply)
        case reply: ReservationReply.Failure ⇒
          reply.replyTo ! reply
      }
  }

  override def receive: Receive =
    write /*orElse read*/ orElse {
      case scala.util.Failure(ex) ⇒
        log.error(ex, "Unexpected error")
    }
}