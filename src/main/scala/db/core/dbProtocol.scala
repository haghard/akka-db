package db.core

sealed trait DBOps

case class WriteOp(id: Long) extends DBOps