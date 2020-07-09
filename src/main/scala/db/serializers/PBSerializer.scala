/*
package db.serializers

import akka.actor.ExtendedActorSystem
import akka.serialization.SerializerWithStringManifest
import com.rbmhtechnology.eventuate.crdt.MVRegister
import com.rbmhtechnology.eventuate.{VectorTime, Versioned}

class PBSerializer(val system: ExtendedActorSystem) extends SerializerWithStringManifest {
  override val identifier: Int = 99999

  override def manifest(obj: AnyRef): String = obj.getClass.getName

  override def toBinary(obj: AnyRef): Array[Byte] =
    obj match {
      case vt: VectorTime ⇒
        val bts = db.core.serialization.protocolV0.VectorTimePB(vt.value).toByteArray
        //println(s"toBinary: VectorTime ${vt.value.mkString(",")} Size:${bts.size}")
        bts
      case r: MVRegister[String] ⇒
        val a: Seq[db.core.serialization.protocolV0.VersionedPB] =
          r.versioned.map { v ⇒
            db.core.serialization.protocolV0
              .VersionedPB(v.value, Some(db.core.serialization.protocolV0.VectorTimePB(v.vectorTimestamp.value)))
          }.toSeq

        val bts = db.core.serialization.protocolV0.MVRegisterPB(a).toByteArray
        //println(s"toBinary: MVRegister ${r.value.mkString(",")} Size:${bts.size}")
        bts
      case _ ⇒
        throw new IllegalStateException(
          s"Serialization for $obj not supported. Check toBinary in ${this.getClass.getName}."
        )
    }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    if (manifest == classOf[com.rbmhtechnology.eventuate.crdt.MVRegister[String]].getName) {
      val pb = db.core.serialization.protocolV0.MVRegisterPB.parseFrom(bytes)

      val a: Seq[Versioned[String]] = pb.vts.map { v: db.core.serialization.protocolV0.VersionedPB ⇒
        v.vt.map(x ⇒ Versioned(v.value, VectorTime(x.values))).get
      }
      val obj = new MVRegister(a.toSet)
      //println(s"fromBinary: MVRegister ${obj.value.mkString(",")}")
      obj
    } else if (manifest == classOf[com.rbmhtechnology.eventuate.VectorTime].getName) {
      val pb  = db.core.serialization.protocolV0.VectorTimePB.parseFrom(bytes)
      val obj = VectorTime(pb.values)
      //println(s"fromBinary: VectorTime ${obj.value.mkString(",")}")
      obj
    } else
      throw new IllegalStateException(
        s"Deserialization for $manifest not supported. Check fromBinary method in ${this.getClass.getName} class."
      )
}
 */
