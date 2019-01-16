package app

object Hotel {

  /**
    * Getting integer value in any case
    * @param array Array of strings
    * @param index Array index
    * @return converted from string integer value
    *         In case of any Exception method returns 0
    */
  private def getAsInt(array: Array[String], index: Int): Int = {
    try {
      array(index).toInt
    } catch {
      case e: Exception => 0
    }
  }

  /**
    * Create hotel object from CSV-like string
    * @param row String contains comma-separated values
    * @return Object of Hotel Class
    */
  def createHotel(row: String): Hotel = {

    val hotelElements: Array[String] = row.split(",")

    new Hotel(
      getAsInt(hotelElements,3),
      getAsInt(hotelElements,13),
      getAsInt(hotelElements,14),
      getAsInt(hotelElements,16),
      getAsInt(hotelElements,18),
      getAsInt(hotelElements,20),
      getAsInt(hotelElements,21),
      getAsInt(hotelElements,22),
      getAsInt(hotelElements,23)
    )
  }
}

case class Hotel(user_location_country: Int,
                 srch_adults_cnt: Int,
                 srch_children_cnt: Int,
                 srch_destination_id: Int,
                 is_booking: Int,
                 hotel_continent: Int,
                 hotel_country: Int,
                 hotel_market: Int,
                 hotel_cluster: Int)