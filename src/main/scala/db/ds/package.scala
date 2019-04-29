package db

package object ds {

  type Block = Array[Byte]

  case class Digest(hash: Array[Byte]) extends AnyVal {
    def +(that: Digest): Digest = Digest(this.hash ++ that.hash)
    def ==(that: Digest): Boolean = this.hash.deep == that.hash.deep
  }

  case class MerkleNodeId(id: Int) extends AnyVal

  trait MerkleDigest[T] {
    def digest(t: T): Digest
  }

  object MerkleDigest {

    implicit object CRC32 extends MerkleDigest[Block] {
      override def digest(t: Block): Digest = {
        val digest = new java.util.zip.CRC32()
        digest.update(t)

        val buffer = java.nio.ByteBuffer.allocate(8)
        buffer.putLong(digest.getValue)

        Digest(buffer.array)
      }
    }

    implicit object MD5 extends MerkleDigest[Block] {
      override def digest(t: Block): Digest =
        Digest(java.security.MessageDigest.getInstance("MD5").digest(t))
    }

  }

}
