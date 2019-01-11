package app

import scala.beans.BeanProperty

object Hotel {

  private def getAsString(array: Array[String], index: Int): String = {
    try {
      array(index)
    } catch {
      case e: Exception => ""
    }
  }

  private def getAsInt(array: Array[String], index: Int): Int = {
    try {
      array(index).toInt
    } catch {
      case e: Exception => 0
    }
  }

  private def getAsDouble(array: Array[String], index: Int): Double = {
    try {
      array(index).toDouble
    } catch {
      case e: Exception => 0.0
    }
  }

  private def getAsLong(array: Array[String], index: Int): Long = {
    try {
      array(index).toLong
    } catch {
      case e: Exception => 0L
    }
  }

  def createHotel(row: String): Hotel = {

    val hotelElements: Array[String] = row.split(",")

    val date_time: String = getAsString(hotelElements,0)
    val site_name: Int = getAsInt(hotelElements,1)
    val posa_continent: Int = getAsInt(hotelElements,2)
    val user_location_country: Int = getAsInt(hotelElements,3)
    val user_location_region: Int = getAsInt(hotelElements,4)
    val user_location_city: Int = getAsInt(hotelElements,5)
    val orig_destination_distance: Double = getAsDouble(hotelElements,6)
    val user_id: Int = getAsInt(hotelElements,7)
    val is_mobile: Int = getAsInt(hotelElements,8)
    val is_package: Int = getAsInt(hotelElements,9)
    val channel: Int = getAsInt(hotelElements,10)
    val srch_ci: String = getAsString(hotelElements,11)
    val srch_co: String = getAsString(hotelElements,12)
    val srch_adults_cnt: Int = getAsInt(hotelElements,13)
    val srch_children_cnt: Int = getAsInt(hotelElements,14)
    val srch_rm_cnt: Int = getAsInt(hotelElements,15)
    val srch_destination_id: Int = getAsInt(hotelElements,16)
    val srch_destination_type_id: Int = getAsInt(hotelElements,17)
    val is_booking: Int = getAsInt(hotelElements,18)
    val cnt: Long = getAsLong(hotelElements,19)
    val hotel_continent: Int = getAsInt(hotelElements,20)
    val hotel_country: Int = getAsInt(hotelElements,21)
    val hotel_market: Int = getAsInt(hotelElements,22)
    val hotel_cluster: Int = getAsInt(hotelElements,23)

    new Hotel(
      date_time,
      site_name,
      posa_continent,
      user_location_country,
      user_location_region,
      user_location_city,
      orig_destination_distance,
      user_id,
      is_mobile,
      is_package,
      channel,
      srch_ci,
      srch_co,
      srch_adults_cnt,
      srch_children_cnt,
      srch_rm_cnt,
      srch_destination_id,
      srch_destination_type_id,
      is_booking,
      cnt,
      hotel_continent,
      hotel_country,
      hotel_market,
      hotel_cluster
    )
  }
}

class Hotel(@BeanProperty var date_time: String,
            @BeanProperty var site_name: Int,
            @BeanProperty var posa_continent: Int,
            @BeanProperty var user_location_country: Int,
            @BeanProperty var user_location_region: Int,
            @BeanProperty var user_location_city: Int,
            @BeanProperty var orig_destination_distance: Double,
            @BeanProperty var user_id: Int,
            @BeanProperty var is_mobile: Int,
            @BeanProperty var is_package: Int,
            @BeanProperty var channel: Int,
            @BeanProperty var srch_ci: String,
            @BeanProperty var srch_co: String,
            @BeanProperty var srch_adults_cnt: Int,
            @BeanProperty var srch_children_cnt: Int,
            @BeanProperty var srch_rm_cnt: Int,
            @BeanProperty var srch_destination_id: Int,
            @BeanProperty var srch_destination_type_id: Int,
            @BeanProperty var is_booking: Int,
            @BeanProperty var cnt: Long,
            @BeanProperty var hotel_continent: Int,
            @BeanProperty var hotel_country: Int,
            @BeanProperty var hotel_market: Int,
            @BeanProperty var hotel_cluster: Int)
  extends Serializable {

  override def toString = s"Hotel(date_time=$date_time, site_name=$site_name, posa_continent=$posa_continent, user_location_country=$user_location_country, user_location_region=$user_location_region, user_location_city=$user_location_city, orig_destination_distance=$orig_destination_distance, user_id=$user_id, is_mobile=$is_mobile, is_package=$is_package, channel=$channel, srch_ci=$srch_ci, srch_co=$srch_co, srch_adults_cnt=$srch_adults_cnt, srch_children_cnt=$srch_children_cnt, srch_rm_cnt=$srch_rm_cnt, srch_destination_id=$srch_destination_id, srch_destination_type_id=$srch_destination_type_id, is_booking=$is_booking, cnt=$cnt, hotel_continent=$hotel_continent, hotel_country=$hotel_country, hotel_market=$hotel_market, hotel_cluster=$hotel_cluster)"
}