package Project.Model

class Product(val id: Int, val name: String, val amount: Int, val price: Double, var totalPrice: Double) {

  override def toString = s"$id, $name, $amount, $price, $totalPrice"
}