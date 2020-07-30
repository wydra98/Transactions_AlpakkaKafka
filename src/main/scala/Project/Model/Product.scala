package Project.Model

class Product(val id: Int, val name: String, val amount: Int, val price: Double) {

  override def toString = s"$id, $name, $amount, $price"
}