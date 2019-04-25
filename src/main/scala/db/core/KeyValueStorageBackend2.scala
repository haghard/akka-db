package db.core

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{ Files, Paths }
import java.util.concurrent.atomic.AtomicReference

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.cluster.Cluster
import db.core.DB.{ CGet, CPut, GetFailure, GetResponse, GetSuccess, PutFailure, PutResponse, PutSuccess }
import org.rocksdb.{ CompactionStyle, CompressionType, Options, ReadOptions, TransactionDB, TransactionDBOptions, TransactionOptions, WriteOptions }
import org.rocksdb.util.SizeUnit

import scala.concurrent.Future
import scala.util.Try
import akka.pattern.pipe
import akka.serialization.{ SerializationExtension, Serializer }
import com.rbmhtechnology.eventuate.VectorTime
import KeyValueStorageBackend2._

object KeyValueStorageBackend2 {

  val path = "rocks-db"

  val keyPostfix = ".vt"

  val vtClazz = classOf[com.rbmhtechnology.eventuate.VectorTime]
  val regClazz = classOf[com.rbmhtechnology.eventuate.crdt.MVRegister[String]]

  def props = Props(new KeyValueStorageBackend2)
    .withDispatcher("akka.db-io")
}

/*
    SNAPSHOT ISOLATION
    https://jepsen.io/consistency/models/snapshot-isolation

      When a txn starts, it sees a consistent snapshot of the db that existed at the moment that the txn started.
      If two txns update the same object, then first writer wins.
      We get SI automatically for free with MVCC

     Main benefits of MVCC
      * Writers don't block readers
      * Read-only txns can read a shapshot without acquiring a lock.

     Allows WRITE SKEW
*/

class KeyValueStorageBackend2 extends Actor with ActorLogging {
  org.rocksdb.RocksDB.loadLibrary()

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

  val ser = SerializationExtension(context.system)
  val vtSerRef = new AtomicReference[Serializer](ser.serializerFor(vtClazz))
  val regSerRef = new AtomicReference[Serializer](ser.serializerFor(regClazz))

  override def preStart(): Unit = {
    log.info("db path:{}", dbPath)
  }

  override def postStop(): Unit = {
    log.warning("Stop db on {}", dbPath)
    txnDb.close()
  }

  def put(key: String, value: String, node: Node, replyTo: ActorRef): PutResponse =
    txn.writeTxn(txnDb.beginTransaction(writeOptions, new TransactionOptions().setSetSnapshot(true)), log) { txn ⇒
      val vtSer = vtSerRef.get
      val regSer = regSerRef.get

      val kb = key.getBytes(UTF_8)
      val kb0 = (key + keyPostfix).getBytes(UTF_8)
      val snapshot = txn.getSnapshot
      val readOptions = new ReadOptions().setSnapshot(snapshot)

      /*
        Guarding against Read-Write Conflicts:
          txn.getForUpdate ensures that no other writer modifies any keys that were read by this transaction.
       */
      val prevRegBts = txn.getForUpdate(readOptions, kb, true)
      val prevVtBts = txn.getForUpdate(readOptions, kb0, true)

      if (prevRegBts eq null) {
        val vt = com.rbmhtechnology.eventuate.VectorTime.Zero.increment(node.port.toString)
        val reg = com.rbmhtechnology.eventuate.crdt.MVRegister[String]().assign(value, vt)
        txn.put(kb, regSer.toBinary(reg))
        txn.put(kb0, vtSer.toBinary(vt))
      } else {
        val prevVt = regSer.fromBinary(prevVtBts, Some(vtClazz)).asInstanceOf[VectorTime]
        val prevReg = regSer.fromBinary(prevRegBts, Some(regClazz))
          .asInstanceOf[com.rbmhtechnology.eventuate.crdt.MVRegister[String]]

        val vt = prevVt.increment(node.port.toString)
        val updatedReg = prevReg.assign(value, vt)

        txn.put(kb, regSer.toBinary(updatedReg))
        txn.put(kb0, vtSer.toBinary(vt))
      }
      Right(key)
    }.fold((PutFailure(key, _, replyTo)), PutSuccess(_, replyTo))

  def get(key: String, replyTo: ActorRef): GetResponse = {
    txn.readTxn(txnDb.beginTransaction(writeOptions, new TransactionOptions().setSetSnapshot(true)), log) { txn ⇒
      //val vtSer = vtSerRef.get
      val regSer = regSerRef.get

      val kb = key.getBytes(UTF_8)
      //val kb0 = (key + postfix).getBytes(UTF_8)

      val snapshot = txn.getSnapshot
      val readOptions = new ReadOptions().setSnapshot(snapshot)

      val regBts = txn.get(readOptions, kb)
      //val vtBts = txn.get(readOptions, kb0)

      if (regBts ne null) {
        //val vt = vtSer.fromBinary(vtBts, Some(vtClazz)).asInstanceOf[VectorTime]
        val reg = regSer.fromBinary(regBts, Some(regClazz))
          .asInstanceOf[com.rbmhtechnology.eventuate.crdt.MVRegister[String]]

        Right(Some(reg.value))
        //Right(Some(reg.value.mkString(",") + ": [" + vt.value.mkString(",") + "]"))
      } else Right(None)
    }.fold((GetFailure(key, _, replyTo)), GetSuccess(_, replyTo))
  }

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
        case s: GetSuccess ⇒
          s.replyTo ! s
        case f: GetFailure ⇒
          f.replyTo ! f
      }
  }

  override def receive: Receive = write orElse read orElse {
    case scala.util.Failure(ex) ⇒
      log.error(ex, "Unexpected error")
  }
}
