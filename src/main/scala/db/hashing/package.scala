package db

import java.util
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets._
import java.util.concurrent.{ConcurrentSkipListMap, ConcurrentSkipListSet}

import scala.collection.immutable.SortedSet

package object hashing {

  trait Hashing[Shard] {
    def seed: Long

    def name: String

    def withShards(shard: util.Collection[Shard]): Hashing[Shard] = {
      val iter = shard.iterator
      while (iter.hasNext)
        add(iter.next)
      this
    }

    def toBinary(shard: Shard): Array[Byte]

    def remove(shard: Shard): Boolean

    def add(shard: Shard): Boolean

    def memberFor(key: String, rf: Int): Set[Shard]

    def validated(shard: Shard): Boolean
  }

  /**
    * Highest Random Weight (HRW) hashing
    * https://github.com/clohfink/RendezvousHash
    * https://www.pvk.ca/Blog/2017/09/24/rendezvous-hashing-my-baseline-consistent-distribution-method/
    * A random uniform way to partition your keyspace up among the available nodes
    */
  trait Rendezvous[ShardId] extends Hashing[ShardId] {
    override val seed = 512L
    override val name = "rendezvous-hashing"

    implicit val ord: Ordering[(Long, ShardId)] =
      (x: (Long, ShardId), y: (Long, ShardId)) ⇒ -x._1.compare(y._1)

    protected val members = new ConcurrentSkipListSet[ShardId]()

    override def remove(shard: ShardId): Boolean =
      members.remove(shard)

    override def add(shard: ShardId): Boolean =
      if (validated(shard)) members.add(shard) else false

    override def memberFor(key: String, rf: Int): Set[ShardId] = {
      if (rf > members.size)
        throw new Exception("Replication factor more than the number of the ranges on a ring")

      var candidates = SortedSet.empty[(Long, ShardId)]
      val iter       = members.iterator
      while (iter.hasNext) {
        val shard           = iter.next
        val keyBytes        = key.getBytes(UTF_8)
        val nodeBytes       = toBinary(shard)
        val keyAndShard     = ByteBuffer.allocate(keyBytes.length + nodeBytes.length).put(keyBytes).put(nodeBytes)
        val shardHash128bit = CassandraMurmurHash.hash3_x64_128(keyAndShard, 0, keyAndShard.array.length, seed)(1)
        candidates = candidates + (shardHash128bit → shard)
      }
      candidates.take(rf).map(_._2)(Ordering.by[ShardId, String](_.toString))
    }

    override def toString: String = {
      val iter = members.iterator
      val sb   = new StringBuilder
      while (iter.hasNext) {
        val shard = iter.next
        sb.append(s"[${shard}]").append("->")
      }
      sb.toString
    }
  }

  /*
    https://community.oracle.com/blogs/tomwhite/2007/11/27/consistent-hashing
    https://www.datastax.com/dev/blog/token-allocation-algorithm
    http://docs.basho.com/riak/kv/2.2.3/learn/concepts/vnodes/

    We want to have an even split of the token range so that load can be well distributed between nodes,
      as well as the ability to add new nodes and have them take a fair share of the load without the necessity
      to move data between the existing nodes
   */
  trait Consistent[T] extends Hashing[T] {
    private val numberOfVNodes = 1 << 5
    override val seed          = 512L
    override val name          = "consistent-hashing"

    /*
      val ring = SortedMap[Long, T]()
      ring.keysIteratorFrom()
      ring.iteratorFrom()
     */
    private val ring: java.util.SortedMap[Long, T] = new ConcurrentSkipListMap[Long, T]()

    private def writeInt(arr: Array[Byte], i: Int, offset: Int): Array[Byte] = {
      arr(offset) = (i >>> 24).toByte
      arr(offset + 1) = (i >>> 16).toByte
      arr(offset + 2) = (i >>> 8).toByte
      arr(offset + 3) = i.toByte
      arr
    }

    override def remove(shard: T): Boolean =
      (0 to numberOfVNodes).foldLeft(true) { (acc, vNodeId) ⇒
        val vNodeSuffix = Array.ofDim[Byte](4)
        writeInt(vNodeSuffix, vNodeId, 0)
        val bytes          = toBinary(shard) ++ vNodeSuffix
        val nodeHash128bit = CassandraMurmurHash.hash3_x64_128(ByteBuffer.wrap(bytes), 0, bytes.length, seed)(1)
        acc & shard == ring.remove(nodeHash128bit)
      }

    override def add(shard: T): Boolean =
      //Hash each node to several numberOfVNodes
      if (validated(shard))
        (0 to numberOfVNodes).foldLeft(true) { (acc, i) ⇒
          val suffix = Array.ofDim[Byte](4)
          writeInt(suffix, i, 0)
          val shardBytes = toBinary(shard) ++ suffix
          val nodeHash128bit =
            CassandraMurmurHash.hash3_x64_128(ByteBuffer.wrap(shardBytes), 0, shardBytes.length, seed)(1)
          acc && (ring.put(nodeHash128bit, shard) == null)
        }
      else false

    override def memberFor(key: String, rf: Int): Set[T] = {
      val localRing = ring
      if (rf > localRing.keySet.size / numberOfVNodes)
        throw new Exception("Replication factor more than the number of the ranges in the ring")

      val keyBytes = key.getBytes(UTF_8)
      val keyHash  = CassandraMurmurHash.hash3_x64_128(ByteBuffer.wrap(keyBytes), 0, keyBytes.length, seed)(1)

      var i    = 0
      var res  = Set.empty[T]
      val tail = localRing.tailMap(keyHash).values
      val all  = localRing.values

      val it = tail.iterator
      if (tail.size >= rf) {
        val it = tail.iterator
        while (it.hasNext && i < rf) {
          val n = it.next
          if (!res.contains(n)) {
            i += 1
            res = res + n
          }
        }
        res
      } else {
        while (it.hasNext) {
          val n = it.next
          if (!res.contains(n)) {
            i += 1
            res = res + n
          }
        }
        val it0 = all.iterator
        while (it0.hasNext && i < rf) {
          val n = it0.next
          if (!res.contains(n)) {
            i += 1
            res = res + n
          }
        }
        res
      }
    }

    override def toString: String = {
      val sb   = new StringBuilder
      val iter = ring.entrySet.iterator
      while (iter.hasNext) {
        val n = iter.next
        sb.append(s"[${n.getValue}: ${n.getKey}]").append("->")
      }
      sb.toString
    }
  }

  object Consistent {
    def apply[T: Consistent] = implicitly[Consistent[T]]

    implicit def instance0 =
      new Consistent[String] {
        override def toBinary(node: String): Array[Byte] = node.getBytes(UTF_8)
        override def validated(node: String): Boolean    = true
      }
    implicit def instance1 =
      new Consistent[core.Node] {
        override def toBinary(node: core.Node): Array[Byte] = s"${node.host}:${node.port}".getBytes(UTF_8)
        override def validated(node: core.Node): Boolean    = true
      }
  }

  object Rendezvous {

    def apply[T: Rendezvous] = implicitly[Rendezvous[T]]

    implicit def instance0 =
      new Rendezvous[String] {
        override def toBinary(node: String): Array[Byte] = node.getBytes(UTF_8)
        override def validated(node: String): Boolean    = true
      }

    implicit def instance1 =
      new Rendezvous[db.core.Node] {
        override def toBinary(node: core.Node): Array[Byte] =
          s"${node.host}:${node.port}".getBytes(UTF_8)
        override def validated(node: core.Node): Boolean = true
      }

    implicit def instance2 =
      new Rendezvous[db.core.Replica] {
        override def toBinary(node: core.Replica): Array[Byte] =
          s"${node.addr.host}:${node.addr.port}".getBytes(UTF_8)
        override def validated(shard: core.Replica): Boolean = true
      }
  }
}
