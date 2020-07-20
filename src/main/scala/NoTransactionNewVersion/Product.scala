package NoTransactionNewVersion

class Product(val id: Int, val name: String, val amount: Double, val price: Double){

  override def toString = s"$id, $name, $amount, $price"
}