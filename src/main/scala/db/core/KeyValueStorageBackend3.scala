package db.core

import java.io.File

import akka.actor.{ Actor, ActorLogging, Props }
import akka.cluster.Cluster
import org.rocksdb.Options
import org.rocksdb._
import org.rocksdb.util.SizeUnit
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{ Files, Paths }

import scala.concurrent.Future
import DB._
import akka.actor.typed.receptionist.{ Receptionist, ServiceKey }
import akka.event.LoggingAdapter
import akka.pattern.pipe
import db.core.KeyValueStorageBackend3.{ CPut3, KVResponse3, PutFailure3, PutSuccess3 }

import scala.util.Try
import scala.util.control.NonFatal
import KeyValueStorageBackend3._

object KeyValueStorageBackend3 {

  sealed trait KVRequest3

  case class CPut3(key: String, value: String, replyTo: akka.actor.typed.ActorRef[KVResponse3]) extends KVRequest3

  sealed trait KVResponse3

  case class PutSuccess3(key: String, replyTo: akka.actor.typed.ActorRef[KVResponse3]) extends KVResponse3

  case class PutFailure3(key: String, th: Throwable, replyTo: akka.actor.typed.ActorRef[KVResponse3]) extends KVResponse3

  val sbKey = ServiceKey[KVProtocol]("StorageBackend")

  val sep = ','
  val path = "rocks-db"
  val ticketsNum = 200

  def managedIter(r: RocksIterator, log: LoggingAdapter)(f: RocksIterator ⇒ Unit) =
    try f(r)
    catch {
      case NonFatal(ex) ⇒ log.error(ex, "RocksIterator error:")
    } finally r.close

  def props(r: akka.actor.typed.ActorRef[Receptionist.Command]) =
    Props(new KeyValueStorageBackend3(r)).withDispatcher("akka.db-io")
}

/*

https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/TransactionSample.java
https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/OptimisticTransactionSample.java
https://github.com/facebook/rocksdb/wiki/Transactions
https://github.com/facebook/rocksdb/wiki/Merge-Operator
https://github.com/facebook/rocksdb/blob/a283800616cb5da5da43d878037e6398cccf9090/java/src/test/java/org/rocksdb/RocksDBTest.java

    This example: Sell N tickets concurrency problem

    SNAPSHOT ISOLATION (Can't be totally available)
    https://jepsen.io/consistency/models/snapshot-isolation

      When a txn starts, it sees a consistent snapshot of the db that existed at the moment that the txn started.
      If two txns update the same object, then first writer wins.
      We get SI automatically for free with MVCC

     Main benefits of MVCC
      * Writers don't block readers
      * Read-only txns can read a shapshot without acquiring a lock.

     Allows WRITE SKEW anomaly in general. However, it's impossible
      to run into WRITE SKEW in this example because we should have at least two variables
*/

//https://github.com/facebook/rocksdb/tree/master/java/src/main/java/org/rocksdb
class KeyValueStorageBackend3(receptionist: akka.actor.typed.ActorRef[Receptionist.Command]) extends Actor with ActorLogging {
  org.rocksdb.RocksDB.loadLibrary()

  import akka.actor.typed.scaladsl.adapter._

  receptionist ! akka.actor.typed.receptionist.Receptionist.Register(KeyValueStorageBackend.serviceKey, self)

  Try(Files.createDirectory(Paths.get(s"./$path")))

  val cluster = Cluster(context.system)
  val sa = cluster.selfAddress

  implicit val ec = context.system.dispatchers.lookup("akka.db-io")

  val dbPath = new File(s"./$path/replica-${cluster.selfAddress.port.get}")
    .getAbsolutePath

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
    KeyValueStorageBackend3.managedIter(txnDb.newIterator(new ReadOptions()), log) { iter ⇒
      iter.seekToFirst
      while (iter.isValid) {
        val key = new String(iter.key, UTF_8)
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

  def put(key: String, value: String, replyTo: akka.actor.typed.ActorRef[KVResponse3]): KVResponse3 =
    txn.writeTxn(txnDb.beginTransaction(writeOptions, new TransactionOptions().setSetSnapshot(true)), log) { txn ⇒
      val kb = key.getBytes(UTF_8)

      /*
        Guarding against Read-Write Conflicts:
          txn.getForUpdate ensures that no other writer modifies any keys that were read by this transaction.
       */

      val snapshot = txn.getSnapshot
      val salesBts = txn.getForUpdate(new ReadOptions().setSnapshot(snapshot), kb, true)
      val sales = Try(new String(salesBts, UTF_8).split(sep)).getOrElse(Array.empty[String])

      //use merge to activate org.rocksdb.StringAppendOperator
      if (sales.size < ticketsNum) txn.merge(key.getBytes(UTF_8), value.getBytes(UTF_8))
      else throw db.core.txn.InvariantViolation(s"Key ${key}. All tickets have been sold")
      Right(key)
    }.fold((PutFailure3(key, _, replyTo)), PutSuccess3(_, replyTo))

  def write: Receive = {
    case CPut3(key, value, replyTo) ⇒
      Future(put(key, value, replyTo)).mapTo[KVResponse3].pipeTo(self)
    case r: KVResponse3 ⇒
      r match {
        case s: PutSuccess3 ⇒
          s.replyTo ! s
        case f: PutFailure3 ⇒
          f.replyTo ! f
      }
  }

  override def receive: Receive = write /*orElse read*/ orElse {
    case scala.util.Failure(ex) ⇒
      log.error(ex, "Unexpected error")
  }
}