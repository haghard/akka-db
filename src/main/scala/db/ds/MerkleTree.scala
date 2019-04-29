package db.ds

import scala.annotation.tailrec
import scala.util.Try

sealed trait TempMerkleTree {
  def digest: Digest
}

case class TempMerkleHashNode(digest: Digest, left: TempMerkleTree, right: TempMerkleTree) extends TempMerkleTree

case class TempMerkleLeaf(digest: Digest) extends TempMerkleTree

sealed trait MerkleTree {
  def nodeId: MerkleNodeId

  def digest: Digest
}
case class MerkleHashNode(nodeId: MerkleNodeId, digest: Digest, left: MerkleTree, right: MerkleTree) extends MerkleTree
case class MerkleLeaf(nodeId: MerkleNodeId, digest: Digest) extends MerkleTree

object MerkleTree {

  def unapply(blocks: Seq[Block])(implicit ev: MerkleDigest[Block]): Option[MerkleTree] =
    unapply(blocks.toArray)

  def unapply(blocks: Array[Block])(implicit ev: MerkleDigest[Block]): Option[MerkleTree] = {

    def blockToLeaf(b: Block): TempMerkleLeaf =
      TempMerkleLeaf(ev.digest(b))

    def buildTree(blocks: Array[Block]) = Try {
      val leafs = blocks.map(blockToLeaf)
      var trees: Seq[TempMerkleTree] = leafs.toSeq

      while (trees.length > 1) {
        trees = trees.grouped(2)
          .map(x ⇒ mergeTrees(x(0), x(1)))
          .toSeq
      }
      trees.head
    }

    def mergeTrees(n1: TempMerkleTree, n2: TempMerkleTree) = {
      val mergedDigest = n1.digest + n2.digest
      val hash = ev.digest(mergedDigest.hash)
      TempMerkleHashNode(hash, n1, n2)
    }

    def toFinalForm(tmt: TempMerkleTree): MerkleTree = {
      var counter = -1

      def toMerkle(mt: TempMerkleTree): MerkleTree = {
        counter += 1
        mt match {
          case TempMerkleHashNode(digest, left, right) ⇒
            MerkleHashNode(MerkleNodeId(counter), digest, toMerkle(left), toMerkle(right))
          case TempMerkleLeaf(digest) ⇒
            MerkleLeaf(MerkleNodeId(counter), digest)
        }
      }

      toMerkle(tmt)
    }

    buildTree(blocks ++ zeroed(blocks))
      .toOption
      .map(toFinalForm)
  }

  def zeroed(blocks: Seq[Block]): Array[Array[Byte]] = {
    def zero(i: Int): Int = {
      val factor = 2
      var x = factor
      while (x < i) x *= factor
      x - i
    }

    Array.fill(zero(blocks.length))(Array[Byte](0))
  }

  @tailrec
  def findNode(nodeId: MerkleNodeId, merkleTree: MerkleTree): Option[MerkleTree] = {
    merkleTree match {
      case _ if merkleTree.nodeId == nodeId ⇒ Option(merkleTree)
      case MerkleHashNode(nId, _, _, right) if nodeId.id >= right.nodeId.id ⇒ findNode(nodeId, right)
      case MerkleHashNode(nId, _, left, _) ⇒ findNode(nodeId, left)
      case _ ⇒ None
    }
  }
}

/*
it should "have the same top hash" in {
    val blocks: Seq[Block] = Seq(
      Array[Byte](1,2,3),
      Array[Byte](4,5,6),
      Array[Byte](7,8,9),
      Array[Byte](10,11,12)
    )
    val blocks2: Seq[Block] = Seq(
      Array[Byte](1,2,3),
      Array[Byte](4,5,6),
      Array[Byte](7,8,9),
      Array[Byte](10,11,12)
    )

    val digest1 = MerkleTree.unapply(blocks)(MerkleDigest.CRC32).get.digest
    val digest2 = MerkleTree.unapply(blocks2)(MerkleDigest.CRC32).get.digest

    digest1.hash.deep shouldBe digest2.hash.deep
  } F
 */ 