package NoTransaction

class Product(val id: Int, val name: String, val amount: Int, val price: Double, val position: Int){

  override def toString = s"$id, $name, $amount, $price, $position"
}