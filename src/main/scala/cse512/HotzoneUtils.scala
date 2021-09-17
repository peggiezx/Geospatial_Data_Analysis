package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    val rectArray = queryRectangle.split(",").map(_.toDouble)
    val pointArray = pointString.split(",").map(_.toDouble)
    val intx = rectArray(0)<=pointArray(0) & rectArray(2)>= pointArray(0)
    val inty = rectArray(1)<=pointArray(1) & rectArray(3)>= pointArray(1)

    return intx & inty // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
