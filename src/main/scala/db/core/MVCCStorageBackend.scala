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
import akka.serialization.SerializationExtension
import db.core.MVCCStorageBackend.{ReservationReply, Reserve, _}
import org.rocksdb.{Options, _}
import org.rocksdb.util.SizeUnit

import scala.concurrent.Future
import scala.util.Try
import scala.util.control.NonFatal

/*

Sources:
https://www.cockroachlabs.com/blog/cockroachdb-on-rocksd/
https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/

Mapping SQL table data to keys and values is encoding typed column data into strings.


CREATE TABLE products (
  key       INT PRIMARY KEY,
  amount    FLOAT,
  prodName  STRING
)


Key	                  Value
/products/10/amount	    4
/products/10/prodName   "”


<table name> / primary key / <column id>       value



If we were to look under the hood, we would see the table metadata:

products Table ID	  - 1000
key      Column ID	- 1
amount   Column ID	- 2
prodName Column ID	- 3


In numeric form, the key-value pairs for our table look like:

Key	Value
/1000/10/2	4.5
/1000/10/3	"hello”


The query:
SELECT * FROM products WHERE key = 10

Will be translated into: Scan(/test/10/, /test/10/Ω)


RocksDB supports
 * scans over a range [start, end)
 * delete a range over [start,end),
 * bulk ingest a set of keys and values


A feature "Prefix bloom filter", allows the bloom filter to be constructed on a prefix of a key.
https://rockset.com/blog/how-we-use-rocksdb-at-rockset/
https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter
https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter#prefix-vs-whole-key



We usually simply seek to a key using a smaller, shared prefix of the key.
Therefore, we set BlockBasedTableOptions::whole_key_filtering to false
so that whole keys are not used to populate and thereby pollute the bloom filters created for each SST.
We also use a custom ColumnFamilyOptions::prefix_extractor so that only the useful prefix of the key is used for
constructing the bloom filters.


By default a hash of every whole key is added to the bloom filter. This can be disabled by setting BlockBasedTableOptions::whole_key_filtering to false.
When Options.prefix_extractor is set, a hash of the prefix is also added to the bloom. Since there are less unique prefixes than unique whole keys,
storing only the prefixes in bloom will result into smaller blooms with the down side of having larger false positive rate.
Moreover the prefix blooms can be optionally (using check_filter when creating the iterator) also used during ::Seek and ::SeekForPrev whereas the whole key
blooms are only used for point lookups.



https://github.com/facebook/rocksdb/wiki/Prefix-Seek#configure-prefix-bloom-filter
new ReadOptions().setTotalOrderSeek(true)

 */

object MVCCStorageBackend {
  val ticketsNum = 200
  val path       = "rocks-db"

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

  def managedIter(rocksIterator: RocksIterator, log: LoggingAdapter)(f: RocksIterator ⇒ Unit) =
    try f(rocksIterator)
    catch {
      case NonFatal(ex) ⇒ log.error(ex, "RocksIterator error:")
    } finally rocksIterator.close

  def props(receptionist: ActorRef[Receptionist.Command]) =
    Props(new MVCCStorageBackend(receptionist)).withDispatcher("akka.db-io")
}

/*

https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/TransactionSample.java
https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/OptimisticTransactionSample.java
https://github.com/facebook/rocksdb/wiki/Transactions
https://github.com/facebook/rocksdb/wiki/Merge-Operator
https://github.com/facebook/rocksdb/blob/a283800616cb5da5da43d878037e6398cccf9090/java/src/test/java/org/rocksdb/RocksDBTest.java


SNAPSHOT ISOLATION
In a snapshot isolated system, each transaction appears to operate on an independent, consistent snapshot of the database.
Its changes are visible only to that transaction until commit time, when all changes become visible atomically.
If transaction T1 has modified an object x, and another transaction T2 committed a write to x after T1’s snapshot began, and before T1’s commit,
then T1 must abort.
(Can't be totally available) https://jepsen.io/consistency/models/snapshot-isolation

Example: Sell N vouchers without overselling.
It's a CAP-sensitive invariant(req coordination. "Two way" communication)

This is impossible to achieve in distributed env with SNAPSHOT ISOLATION on each replica.
We need SERIALIZABLE (or LINEARIZABLE) isolation here.
So this is just a toy example.

  When a txn starts, it sees a consistent snapshot of the db that existed at the moment that the txn started.
  If two txns update the same object, then first writer wins.
  We get SI automatically for free with MVCC

Main benefits of MVCC/Shapshot Isolation
 * Writers don't block readers
 * Read-only txns can read a shapshot without acquiring a lock.

 Allows WRITE SKEW anomaly in general. However, it's impossible
  to run into WRITE SKEW in this example inside a single process because in order to get it our transaction should touch two or more keys



Next best isolation level that can be build in top of is MVCC/Shapshot isolation in Causal consistency (AntidoteDB).
Causal consistency is a stronger consistency model that ensures that operations are processed in the expected order.
Causal consistency is the strongest method of consistency achievable while retaining availability. More precisely,
partial order over operations is enforced through metadata. If operation A occurs before operation B, for example,
any data store that sees operation B must see operation A first.

Three rules define potential causality:
The most common way to implement causal consistency in an Akka-based actor model is through Become/Unbecome then you buffer
incoming events, or via the Process Manager pattern.


 */

//https://github.com/facebook/rocksdb/tree/master/java/src/main/java/org/rocksdb

//MVCC simply means that you don't override a value when you write the same key twice
final class MVCCStorageBackend(receptionist: ActorRef[Receptionist.Command]) extends Actor with ActorLogging {
  val ser         = SerializationExtension(context.system)
  implicit val ec = context.system.dispatchers.lookup("akka.db-io")

  val SEPARATOR = ';'
  val cluster   = Cluster(context.system)
  val dbPath    = new File(s"./$path/replica-${cluster.selfAddress.port.get}").getAbsolutePath

  //BlockBasedTableOptions::whole_key_filtering to false

  val options = new Options()
    .setCreateIfMissing(true)
    .setWriteBufferSize(10 * SizeUnit.KB)
    .setMaxWriteBufferNumber(3)
    .setMaxSubcompactions(10)
    .setMaxBackgroundJobs(3)
    //https://github.com/facebook/rocksdb/wiki/Merge-Operator
    //why? Allows for Atomic Read-Modify-Write scenario. Works in combination with txn.merge
    .setMergeOperator(
      //new org.rocksdb.CassandraValueMergeOperator(5000) TODO: Try with new version
      new org.rocksdb.StringAppendOperator(SEPARATOR)
    ) //new CassandraValueMergeOperator() didn't work.
    .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
    .setCompactionStyle(CompactionStyle.UNIVERSAL)
  //A CompactionFilter allows an application to modify/delete a key-value at the time of compaction
  //.setCompactionFilter(???)
  //.setCompactionFilterFactory(AbstractCompactionFilterFactory)}

  //By default a hash of every whole key is added to the bloom filter. This can be disabled by setting BlockBasedTableOptions::whole_key_filtering to false
  //.setTableFormatConfig(new BlockBasedTableConfig().setWholeKeyFiltering(false))
  // Define a prefix. In this way, a fixed length prefix extractor. A recommended one to use.
  //.useFixedLengthPrefixExtractor(3)

  //Putting read_options.total_order_seek = true will make sure the query returns the same result as if there is no prefix bloom filter.
  //new ReadOptions().setTotalOrderSeek(false)

  val txnDbOptions = new TransactionDBOptions()
  val writeOptions = new WriteOptions()

  val txnDb: TransactionDB =
    TransactionDB.open(options, txnDbOptions, dbPath)

  override def preStart(): Unit = {
    Files.createDirectory(Paths.get(s"./$path"))
    org.rocksdb.RocksDB.loadLibrary()

    log.info("DB file path: {}", dbPath)

    receptionist ! akka.actor.typed.receptionist.Receptionist.Register(MVCCStorageBackend.Key, self)

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

        //READ to know how many vouchers have been sold
        val salesBts = txn.getForUpdate(new ReadOptions().setSnapshot(snapshot), keyBytes, true)

        //txn.getIterator(new ReadOptions().setSnapshot(snapshot))

        //get multiple keys txn.multiGetForUpdate(new ReadOptions().setSnapshot(snapshot), Array(keyBytes, keyBytes))
        //https://github.com/facebook/rocksdb/blob/189f0c27aaecdf17ae7fc1f826a423a28b77984f/java/src/test/java/org/rocksdb/OptimisticTransactionTest.java#L96
        //if at least one key from that set of keys was modified between multiGetForUpdate, put|merge and commit,
        //we get RocksDBException with Status.Code.Busy

        //get multiple keys

        //txn.multiGetForUpdate(new ReadOptions().setSnapshot(snapshot), Array(keyBytes, keyBytes))

        val sales = Try(new String(salesBts, UTF_8).split(SEPARATOR)).getOrElse(Array.ofDim[String](0))

        //WRITE sell if some left
        if (sales.size < ticketsNum) {
          //merge activates org.rocksdb.StringAppendOperator
          txn.merge(key.getBytes(UTF_8), value.getBytes(UTF_8))
          Some(key)
        } else None
      }
      .fold(
        ReservationReply.Failure(key, _, replyTo),
        _.fold[ReservationReply](ReservationReply.Closed(key, replyTo))(ReservationReply.Success(_, replyTo))
      )

  def write: Receive = {
    case Reserve(key, value, replyTo) ⇒
      Future(put(key, value, replyTo)).mapTo[ReservationReply].pipeTo(self)
    case r: ReservationReply ⇒
      r match {
        case reply: ReservationReply.Success ⇒
          reply.replyTo.tell(reply)
        case reply: ReservationReply.Failure ⇒
          reply.replyTo ! reply
        case reply: ReservationReply.Closed ⇒
          reply.replyTo ! reply
      }
  }

  override def receive: Receive =
    write /*orElse read*/ orElse {
      case scala.util.Failure(ex) ⇒
        log.error(ex, "Unexpected error")
    }
}
