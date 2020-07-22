package SourceProducent

class UniqueId {

  var id = 0

  def getId: Int = {
    id
  }

  def updateId(): Unit = {
    id = id + 1
  }
}
