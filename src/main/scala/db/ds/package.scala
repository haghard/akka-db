package db

package object ds {

  type Block = Array[Byte]

  final case class Digest(hash: Array[Byte]) extends AnyVal { self ⇒

    def ++(that: Digest) = Digest(self.hash ++ that.hash)

    private def hex2bytes(hex: String): Array[Byte] =
      hex
        .replaceAll("[^0-9A-Fa-f]", "")
        .sliding(2, 2)
        .toArray
        .map(Integer.parseInt(_, 16).toByte)

    private def bytes2Hex(bytes: Array[Byte]): String = {
      val sb = new StringBuilder
      bytes.foreach(b ⇒ sb.append(String.format("%02X", b: java.lang.Byte)))
      sb.toString
    }

    //com.google.common.io.BaseEncoding.base16().lowerCase().encode(self.hash)
    override def toString: String = bytes2Hex(self.hash)
  }

  final case class NodeId(v: Int) extends AnyVal

  trait MerkleDigest[T] {
    def digest(t: T): Digest
  }

  object MerkleDigest {

    implicit object CRC32 extends MerkleDigest[Block] {
      override def digest(t: Block): Digest = {
        val digest = new java.util.zip.CRC32()
        digest.update(t)
        val bb = java.nio.ByteBuffer.allocate(8)
        bb.putLong(digest.getValue)
        Digest(bb.array)
      }
    }

    implicit object MD5 extends MerkleDigest[Block] {
      override def digest(t: Block): Digest =
        Digest(java.security.MessageDigest.getInstance("MD5").digest(t))
    }
  }
}
