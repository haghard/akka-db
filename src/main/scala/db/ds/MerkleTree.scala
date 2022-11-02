package db.ds

import scala.annotation.tailrec
import scala.util.Try

sealed trait MerkleTreeView {
  def digest: Digest
}

final case class Node(digest: Digest, left: MerkleTreeView, right: MerkleTreeView) extends MerkleTreeView

final case class Leaf(digest: Digest) extends MerkleTreeView

sealed trait MerkleTree {
  def nodeId: NodeId

  def digest: Digest
}

final case class TreeNode(nodeId: NodeId, digest: Digest, left: MerkleTree, right: MerkleTree) extends MerkleTree

final case class TreeLeaf(nodeId: NodeId, digest: Digest) extends MerkleTree

//https://speedcom.github.io/dsp2017/2017/05/14/justindb-active-anti-entropy.html
//https://github.com/justin-db/JustinDB/blob/844a3f6f03192ff3e8248a15712fecd754e06fbc/justin-core/src/main/scala/justin/db/merkletrees/MerkleTree.scala
object MerkleTree {

  def fromArrays(blocks: Seq[Block])(implicit ev: MerkleDigest[Block]): Option[MerkleTree] =
    fromArray(blocks.toArray)

  def fromArray(blocks: Array[Block])(implicit dMaker: MerkleDigest[Block]): Option[MerkleTree] = {

    def blockToLeaf(b: Block): Leaf = Leaf(dMaker.digest(b))

    def buildTree(blocks: Array[Block]) =
      Try {
        val leafs                      = blocks.map(blockToLeaf)
        var trees: Seq[MerkleTreeView] = leafs.toSeq

        while (trees.length > 1)
          trees = trees.grouped(2).map(x => combine(x(0), x(1))).toSeq

        trees.head
      }

    def combine(n1: MerkleTreeView, n2: MerkleTreeView): Node = {
      val mergedDigest = n1.digest ++ n2.digest
      val hash         = dMaker.digest(mergedDigest.hash)
      Node(hash, n1, n2)
    }

    def toFinalForm(tmt: MerkleTreeView): MerkleTree = {
      var counter = -1

      def toMerkle(mt: MerkleTreeView): MerkleTree = {
        counter += 1
        mt match {
          case Node(digest, left, right) =>
            TreeNode(NodeId(counter), digest, toMerkle(left), toMerkle(right))
          case Leaf(digest) =>
            TreeLeaf(NodeId(counter), digest)
        }
      }

      toMerkle(tmt)
    }

    buildTree(blocks ++ zeroed(blocks)).toOption
      .map(toFinalForm)
  }

  def zeroed(blocks: Seq[Block]): Array[Array[Byte]] = {
    def zero(i: Int): Int = {
      val factor = 2
      var x      = factor
      while (x < i) x *= factor
      x - i
    }

    Array.fill(zero(blocks.length))(Array[Byte](0))
  }

  @tailrec def nodeById(nodeId: NodeId, merkleTree: MerkleTree): Option[MerkleTree] =
    if (merkleTree.nodeId == nodeId) Some(merkleTree)
    else
      merkleTree match {
        case TreeNode(_, _, left, right) =>
          if (nodeId.v >= right.nodeId.v) nodeById(nodeId, right) else nodeById(nodeId, left)
        case _ => None
      }

}

object Runner extends App {
  // Seq[Block]

  val blocks = Array(
    Array[Byte](1, 2, 3),
    Array[Byte](4, 5, 6),
    Array[Byte](7, 8, 9),
    Array[Byte](10, 11, 12)
    // Array[Byte](20, 21, 22)
    // Array[Byte](30, 31, 32),
    // Array[Byte](40, 41, 42),
  )

  val blocks2 = Array(
    Array[Byte](1, 2, 3),
    Array[Byte](4, 5, 6),
    Array[Byte](7, 8, 9),
    Array[Byte](10, 11, 121)
    // Array[Byte](20, 21, 23)
    // Array[Byte](30, 31, 32),
    // Array[Byte](40, 41, 42),
  )

  val blocks3 = Array(
    Array[Byte](1, 2, 3),
    Array[Byte](4, 5, 6),
    Array[Byte](7, 8, 9),
    Array[Byte](10, 11, 12)
  )

  // MerkleTree.fromArrays(blocks3)(MerkleDigest.MD5)

  /*
    4 keys means the tree with contain 6 hashes
         _____1_____
        /           \
     __2__         __6__
    /     \       /     \
 3(h1)   4(h2)  7(h3)   8(h4)

   */
  val tree1 = MerkleTree.fromArrays(blocks)(MerkleDigest.CRC32).get
  val tree2 = MerkleTree.fromArrays(blocks2)(MerkleDigest.CRC32).get
  println(tree1)
  println(tree2)

  val one = NodeId(1)
  val a   = MerkleTree.nodeById(one, tree1).get.digest
  val b   = MerkleTree.nodeById(one, tree2).get.digest

  // println(a.toString)
  // println(b.toString)
  println(java.util.Arrays.equals(a.hash, b.hash))

  val two = NodeId(6)
  val c   = MerkleTree.nodeById(two, tree1).get.digest
  val d   = MerkleTree.nodeById(two, tree2).get.digest
  println(java.util.Arrays.equals(c.hash, d.hash))

  val digest1 = tree1.digest
  val digest2 = tree2.digest

  // println(digest1.hash.mkString(","))

  // If the hash values of the root of two trees are equal, then its meaning that leaf nodes are equal
  // (there is no point in doing synchronization since)

  digest1.hash sameElements digest2.hash
  val r = java.util.Arrays.equals(digest1.hash, digest2.hash)
  println(r)
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
  }
 */
