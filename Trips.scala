package ca.mcit.bigdata

case class Trips(  route_id: Int,
                   service_id: String,
                   trip_id: String,
                   trip_headSign: String,
                   direction_id: Int,
                   shape_id: Int,
                   wheelchair_accessible: Int,
                   note_fr: Option[String],
                   note_en: Option[String]
                )

object Trips {
  def toCsv(trip: Trips): String = {
    trip.route_id + "," +
      trip.service_id + "," +
      trip.trip_id + "," +
      trip.trip_headSign + "," +
      trip.direction_id + "," +
      trip.shape_id + "," +
      trip.wheelchair_accessible + "," +
      trip.note_fr.getOrElse("") + "," +
      trip.note_en.getOrElse("")
  }
}

case class routeTrips(trip: Trips, route: Option[Route])

case class EnrichedTrip(routeTrips: routeTrips, calendar: Option[Calendar])
