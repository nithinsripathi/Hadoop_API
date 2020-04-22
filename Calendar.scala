package ca.mcit.bigdata

case class Calendar(service_id: String,
                    monday: Int,
                    tuesday: Int ,
                    wednesday: Int ,
                    thursday: Int ,
                    friday: Int ,
                    saturday: Int ,
                    sunday: Int ,
                    start_date: Int ,
                    end_date: Int
                   )
object Calendar{
  def apply(csvLine: String): Calendar = {
    val c = csvLine.split(",")
    new Calendar(c(0), c(1).toInt, c(2).toInt, c(3).toInt, c(4).toInt, c(5).toInt, c(6).toInt,c(7).toInt, c(8).toInt, c(9).toInt)
  }
  def toCsv(calender: Calendar): String = {
    calender.service_id + "," +
      calender.monday + "," +
      calender.tuesday + "," +
      calender.wednesday + "," +
      calender.thursday + "," +
      calender.friday + "," +
      calender.saturday + "," +
      calender.sunday + "," +
      calender.start_date + "," +
      calender.end_date
  }
}