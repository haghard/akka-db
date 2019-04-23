package db.core

import java.io.File

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.cluster.Cluster
import org.rocksdb.Options
import org.rocksdb._
import org.rocksdb.util.SizeUnit
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{ Files, Paths }

import scala.concurrent.Future
import DB._
import akka.actor.typed.receptionist.{ Receptionist, ServiceKey }
import akka.pattern.pipe
import db.Runner
import db.core.KeyValueStorageBackend3.{ CPut3, PutFailure3, PutResponse3, PutSuccess3 }

import scala.util.Try

object KeyValueStorageBackend3 {

  sealed trait KVRequest3
  case class CPut3(key: String, value: String, replyTo: akka.actor.typed.ActorRef[PutResponse3]) extends KVRequest3

  sealed trait PutResponse3
  case class PutSuccess3(key: String, replyTo: akka.actor.typed.ActorRef[PutResponse3]) extends PutResponse3
  case class PutFailure3(key: String, th: Throwable, replyTo: akka.actor.typed.ActorRef[PutResponse3]) extends PutResponse3

  val serviceKey = ServiceKey[KVProtocol]("db-ops")

  def props(r: akka.actor.typed.ActorRef[Receptionist.Command]) =
    Props(new KeyValueStorageBackend3(r)).withDispatcher("akka.db-io")
}

/*

https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/TransactionSample.java
https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/OptimisticTransactionSample.java

    This example: Sell N tickets concurrency

    SNAPSHOT ISOLATION (Can't be totaly available)
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

class KeyValueStorageBackend3(receptionist: akka.actor.typed.ActorRef[Receptionist.Command]) extends Actor with ActorLogging {
  org.rocksdb.RocksDB.loadLibrary()

  import akka.actor.typed.scaladsl.adapter._

  receptionist ! akka.actor.typed.receptionist.Receptionist.Register(KeyValueStorageBackend.serviceKey, self)

  val path = "rocks-db"

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
    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
    .setCompactionStyle(CompactionStyle.UNIVERSAL)

  val txnDbOptions = new TransactionDBOptions()
  val writeOptions = new WriteOptions()

  val txnDb: TransactionDB =
    TransactionDB.open(options, txnDbOptions, dbPath)

  override def preStart(): Unit = {
    log.info("dbPath:{}", dbPath)
  }

  override def postStop(): Unit = {
    log.warning("Stop db node {}", dbPath)
    txnDb.close()
  }

  def put(key: String, value: String, replyTo: akka.actor.typed.ActorRef[PutResponse3]): PutResponse3 =
    txn.writeTxn(txnDb.beginTransaction(writeOptions, new TransactionOptions().setSetSnapshot(true)), log) { txn ⇒
      val kb = key.getBytes(UTF_8)
      val snapshot = txn.getSnapshot
      val readOptions = new ReadOptions().setSnapshot(snapshot)
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
    }.fold((PutFailure3(key, _, replyTo)), PutSuccess3(_, replyTo))

  /*def get(key: String, replyTo: ActorRef): GetResponse = {
    txn.readTxn0(txnDb.beginTransaction(writeOptions, new TransactionOptions().setSetSnapshot(true)), log) { txn ⇒
      val snapshot = txn.getSnapshot
      val readOptions = new ReadOptions().setSnapshot(snapshot)
      val valueBts = txn.get(readOptions, key.getBytes(UTF_8))
      if (valueBts ne null) Right(Some(new String(valueBts)))
      else Right(None)
    }.fold((GetFailure(key, _, replyTo)), GetSuccess0(_, replyTo))
  }*/

  def write: Receive = {
    case CPut3(key, value, replyTo) ⇒
      Future(put(key, value, replyTo)).mapTo[PutResponse3].pipeTo(self)
    case r: PutResponse3 ⇒
      r match {
        case s: PutSuccess3 ⇒
          s.replyTo ! s
        case f: PutFailure3 ⇒
          f.replyTo ! f
      }
  }

  /*def read: Receive = {
    case CGet3(key) ⇒
      val replyTo = sender()
      Future(get(key, replyTo)).mapTo[GetResponse].pipeTo(self)
    case r: GetResponse ⇒
      r match {
        case s: GetSuccess0 ⇒
          s.replyTo ! s
        case f: GetFailure ⇒
          f.replyTo ! f
      }
  }*/

  override def receive: Receive = write /*orElse read*/ orElse {
    case scala.util.Failure(ex) ⇒
      log.error(ex, "Unexpected error")
  }
}