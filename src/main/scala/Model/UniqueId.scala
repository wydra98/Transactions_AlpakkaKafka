package Model

class UniqueId {

  var id = 0

  def updateAndGetId: Int = {
    id = id + 1
    id
  }
}
