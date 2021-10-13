package Aggregators

case class Quote(val price: Double, val amount: Double, val time : Long) extends Ordered[Quote]{
  override def compare(that: Quote): Int = {
    if(this.price == that.price){
      return 0
    }

    return if(this.price > that.price) 1 else -1
  }
}
