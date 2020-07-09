package db.core

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.Cluster
import org.rocksdb.Options
import org.rocksdb._
import org.rocksdb.util.SizeUnit
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import scala.concurrent.Future
import DB._
import akka.actor.typed.receptionist.ServiceKey
import akka.pattern.pipe
import db.Runner
import db.core.KeyValueStorageBackend3.KVRequest3

import scala.util.Try

object KeyValueStorageBackend {

  val serviceKey = ServiceKey[KVRequest3]("db-ops")

  def props() =
    Props(new KeyValueStorageBackend)
      .withDispatcher("akka.db-io")
}

/*
    This example: Sell N tickets concurrency

    SNAPSHOT ISOLATION (Can't be totally available)
    https://jepsen.io/consistency/models/snapshot-isolation

      When a txn starts, it sees a consistent snapshot of the db that existed at the moment that the txn started.
      If two txns update the same object, then first writer wins and second fails
      We get SI automatically for free with MVCC

     Main benefits of MVCC
 * Writers don't block readers
 * Read-only txns can read a shapshot without acquiring a lock.

     Allows WRITE SKEW anomaly in general. However, it's impossible
      to run into WRITE SKEW in this example because we should have at least two variables
 */

class KeyValueStorageBackend extends Actor with ActorLogging {
  org.rocksdb.RocksDB.loadLibrary()

  val path = "rocks-db"

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
    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
    .setCompactionStyle(CompactionStyle.UNIVERSAL)

  val txnDbOptions = new TransactionDBOptions()
  val writeOptions = new WriteOptions()

  val txnDb: TransactionDB =
    TransactionDB.open(options, txnDbOptions, dbPath)

  override def preStart(): Unit =
    log.info("dbPath:{}", dbPath)

  override def postStop(): Unit = {
    log.warning("Stop db node {}", dbPath)
    txnDb.close
  }

  def put(key: String, value: String, node: Node, replyTo: ActorRef): PutResponse =
    txn
      .writeTxn(txnDb.beginTransaction(writeOptions, new TransactionOptions().setSetSnapshot(true)), log) { txn ⇒
        val kb          = key.getBytes(UTF_8)
        val snapshot    = txn.getSnapshot
        val readOptions = new ReadOptions().setSnapshot(snapshot)
        /*
        Guarding against Read-Write Conflicts:
          txn.getForUpdate ensures that no other writer modifies any keys that were read by this transaction.
         */
        val prevValue = txn.getForUpdate(readOptions, kb, true)
        if (prevValue eq null)
          txn.put(kb, Runner.ticketNmr.toString.getBytes(UTF_8))
        else {
          val prevCounter = new String(prevValue).toInt
          if (prevCounter > 0) {
            val newCounter = (prevCounter - 1).toString
            txn.put(kb, newCounter.getBytes(UTF_8))
          } else throw db.core.txn.InvariantViolation(s"Key ${key} shouldn't go below 0")
        }
        Right(key)
      }
      .fold((PutFailure(key, _, replyTo)), PutSuccess(_, replyTo))

  def get(key: String, replyTo: ActorRef): GetResponse =
    txn
      .readTxn0(txnDb.beginTransaction(writeOptions, new TransactionOptions().setSetSnapshot(true)), log) { txn ⇒
        val snapshot    = txn.getSnapshot
        val readOptions = new ReadOptions().setSnapshot(snapshot)
        val valueBts    = txn.get(readOptions, key.getBytes(UTF_8))
        if (valueBts ne null) Right(Some(new String(valueBts)))
        else Right(None)
      }
      .fold((GetFailure(key, _, replyTo)), GetSuccess0(_, replyTo))

  def write: Receive = {
    case CPut(key, value, node) ⇒
      val replyTo = sender()
      Future(put(key, value, node, replyTo)).mapTo[PutResponse].pipeTo(self)
    case r: PutResponse ⇒
      r match {
        case s: PutSuccess ⇒
          s.replyTo ! s
        case f: PutFailure ⇒
          f.replyTo ! f
      }
  }

  def read: Receive = {
    case CGet(key) ⇒
      val replyTo = sender()
      Future(get(key, replyTo)).mapTo[GetResponse].pipeTo(self)
    case r: GetResponse ⇒
      r match {
        case s: GetSuccess0 ⇒
          s.replyTo ! s
        case f: GetFailure ⇒
          f.replyTo ! f
      }
  }

  override def receive: Receive =
    write orElse read orElse {
      case scala.util.Failure(ex) ⇒
        log.error(ex, "Unexpected error")
    }
}
