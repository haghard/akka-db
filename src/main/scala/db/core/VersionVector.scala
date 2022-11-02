package db.core

import scala.annotation.tailrec
import scala.collection.immutable.SortedMap

/*
  VersionVector have 3 types of relationships

    A >= B - A descends B
    A > B  - A dominates B example [{a:2}, {b:3}] dominates [{a:1}, {b:3}]
    Concurrent A <> B  [{a:1}, {b:0}] <> [{a:0}, {b:1}]
 */
object VersionVector {
  sealed trait Ordering

  case object After      extends Ordering
  case object Before     extends Ordering
  case object Same       extends Ordering
  case object Concurrent extends Ordering

  /** Marker to ensure that we do a full order comparison instead of bailing out early.
    */
  private case object FullOrder extends Ordering

  def empty[A](implicit ord: scala.Ordering[A]) =
    VersionVector[A](SortedMap.empty[A, Long](ord))
}

trait VersionVectorLike[T] {
  type VV <: VersionVectorLike[T]

  /** Increment the version for the node passed as argument. Returns a new VersionVector.
    */
  def +(node: T): VV = increment(node)

  /** Increment the version for the node passed as argument. Returns a new VersionVector.
    */
  protected def increment(node: T): VV

  /** Returns the local view on the logical clock of the given node.
    */
  def version(node: T): Long

  /** Returns true if <code>this</code> and <code>that</code> are concurrent else false.
    */
  def <>(that: VV): Boolean

  /** Returns true if <code>this</code> is before <code>that</code> else false.
    */
  def <(that: VV): Boolean

  /** Returns true if <code>this</code> is after <code>that</code> else false.
    */
  def >(that: VV): Boolean

  /** Returns true if this VersionVector has the same history as the 'that' VersionVector else false.
    */
  def ==(that: VV): Boolean

  /** Computes the union of the nodes and maintains the highest clock value found for each
    */
  def merge(that: VV): VV

  /** Returns the number of nodes registered in this version vector
    */
  protected def size: Int
}

case class VersionVector[T: scala.Ordering](elems: SortedMap[T, Long]) extends VersionVectorLike[T] with Serializable {
  import VersionVector._

  override type VV = VersionVector[T]

  private val ord = implicitly[scala.Ordering[T]]

  /** Increment the version for the node passed as argument. Returns a new VersionVector.
    */
  override protected def increment(node: T): VersionVector[T] =
    VersionVector(elems.updated(node, nodeClock(node) + 1L))

  /** Returns the local view on the logical clock of the given node.
    */
  override def version(node: T): Long = nodeClock(node)

  /** Returns true if <code>this</code> and <code>that</code> are concurrent else false.
    */
  def <>(that: VersionVector[T]): Boolean = compareOnlyTo(that, Concurrent) eq Concurrent

  /** Returns true if <code>this</code> is before <code>that</code> else false.
    */
  def <(that: VersionVector[T]): Boolean = compareOnlyTo(that, Before) eq Before

  /** Returns true if <code>this</code> is after <code>that</code> else false.
    */
  def >(that: VersionVector[T]): Boolean = compareOnlyTo(that, After) eq After

  /** Returns true if this VersionVector has the same history as the 'that' VersionVector else false.
    */
  def ==(that: VersionVector[T]): Boolean = compareOnlyTo(that, Same) eq Same

  /** Version vector comparison according to the semantics described by compareTo, with the ability to bail out early if
    * the we can't reach the Ordering that we are looking for.
    *
    * The ordering always starts with Same and can then go to Same, Before or After If we're on After we can only go to
    * After or Concurrent If we're on Before we can only go to Before or Concurrent If we go to Concurrent we exit the
    * loop immediately
    *
    * If you send in the ordering FullOrder, you will get a full comparison.
    */
  private final def compareOnlyTo(that: VersionVector[T], order: Ordering): Ordering = {
    val requestedOrder = if (order eq Concurrent) FullOrder else order

    @tailrec
    def compare(i1: Seq[(T, Long)], i2: Seq[(T, Long)], currentOrder: Ordering): Ordering = {
      if ((requestedOrder ne FullOrder) && (currentOrder ne Same) && (currentOrder ne requestedOrder)) currentOrder

      (i1, i2) match {
        case (h1 +: t1, h2 +: t2) =>
          // compare the nodes
          val nc = ord.compare(h1._1, h2._1)
          if (nc == 0)
            // both nodes exist compare the timestamps
            // same timestamp so just continue with the next nodes
            if (h1._2 == h2._2) compare(t1, t2, currentOrder)
            else if (h1._2 < h2._2)
              // t1 is less than t2, so i1 can only be Before
              if (currentOrder eq After) Concurrent
              else compare(t1, t2, Before)
            else
            // t2 is less than t1, so i1 can only be After
            if (currentOrder eq Before) Concurrent
            else compare(t1, t2, After)
          else if (nc < 0)
            // this node only exists in i1 so i1 can only be After
            if (currentOrder eq Before) Concurrent
            else compare(t1, h2 +: t2, After)
          else
          // this node only exists in i2 so i1 can only be Before
          if (currentOrder eq After) Concurrent
          else compare(h1 +: t1, t2, Before)

        case (h1 +: t1, _) =>
          // i2 is empty but i1 is not, so i1 can only be After
          if (currentOrder eq Before) Concurrent else After

        case (_, h2 +: t2) =>
          // i1 is empty but i2 is not, so i1 can only be Before
          if (currentOrder eq After) Concurrent else Before

        case _ =>
          currentOrder
      }
    }

    if (this eq that) Same
    else compare(this.elems.view.toSeq, that.elems.view.toSeq, Same)
  }

  /** Computes the union of the nodes and maintains the highest clock value found for each
    */
  override def merge(that: VersionVector[T]): VersionVector[T] = {

    def go(s1: SortedMap[T, Long], s2: SortedMap[T, Long], acc: SortedMap[T, Long]): SortedMap[T, Long] =
      if (s1.nonEmpty && s2.nonEmpty) {
        val (t1, c1) = s1.head
        val (t2, c2) = s2.head
        val r        = ord.compare(t1, t2)

        if (r == 0) {
          // This elements exists only in both maps, take the maximum logical clock value
          val newAcc = acc.updated(t1, math.max(c1, c2))
          go(s1.tail, s2.tail, newAcc)
        } else if (r < 0) {
          // This element exists only in c1
          val newAcc = acc.updated(t1, c1)
          go(s1.tail, s2, newAcc)
        } else {
          // This element exists only in c2
          val newAcc = acc.updated(t2, c2)
          go(s1, s2.tail, newAcc)
        }
      } else if (s1.isEmpty) acc ++ s2
      else acc ++ s1

    val newEntries = go(this.elems, that.elems, SortedMap.empty[T, Long])
    VersionVector(newEntries)
  }

  /** Returns the number of nodes registered in this version vector
    */
  override protected lazy val size: Int = elems.size

  private def nodeClock(node: T): Long = elems.getOrElse(node, 0)
}
