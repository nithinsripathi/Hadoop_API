package ca.mcit.bigdata

import org.apache.hadoop.fs.Path

object Enriched extends Main with App {
    val fileList = fs.listStatus(new Path("/user/fall2019"))
    /*  val tripList = Source
      .fromFile("/home/bd-user/Downloads/trips.txt")
      .getLines()  */
    val tripStream = fs.open(new Path("/user/fall2019/surya/stm/trips.txt"))
    val tripList: List[Trips] = Iterator.continually(tripStream.readLine()).takeWhile(_ != null)
      .toList
      .tail
      .map(_.split(",", -1))
      .map(p => Trips(p(0).toInt, p(1), p(2), p(3), p(4).toInt, p(5).toInt, p(6).toInt,
        if (p(7).isEmpty) None else Some(p(7)),
        if (p(8).isEmpty) None else Some(p(8))))

    /* val routeList: List[Route] = Source
      .fromFile("/home/bd-user/Downloads/routes.txt")
      .getLines() */
    val routeStream = fs.open(new Path("/user/fall2019/surya/stm/routes.txt"))
    val routeList: List[Route] = Iterator.continually(routeStream.readLine()).takeWhile(_ != null)
      .toList
      .tail
      .map(_.split(",", -1))
      .map(r => Route(r(0).toInt, r(1), r(2), r(3), r(4), r(5), r(6), r(7)))

    val calendarStream = fs.open(new Path("/user/fall2019/surya/stm/calendar.txt"))
    val calendar: List[Calendar] = Iterator.continually(calendarStream.readLine()).takeWhile(_ != null)
      .toList
      .tail
      .map(_.split(",", -1))
      .map(c => Calendar(c(0), c(1).toInt, c(2).toInt, c(3).toInt, c(4).toInt, c(5).toInt, c(6).toInt, c(7).toInt, c(8).toInt, c(9).toInt))


    val routeTrip: List[JoinOutput] = new GenericMapJoin[Trips, Route](L => L.route_id.toString)(R => R.route_id.toString)
      .join(tripList, routeList)


    // routeTrip.foreach(println)
    val FinalEnriched: List[JoinOutput] = new GenericNestedLoopJoin[Calendar, JoinOutput]((i, j) =>
      i.service_id == j.left.asInstanceOf[Trips].service_id)
      .join(calendar, routeTrip)

    /* val HadoopEnrichedCsv: List[String] = HadoopEnriched
      .map(JoinOutput => Trip.toCsv(JoinOutput.left.asInstanceOf[Trip]))
  */
    val output: List[String] = FinalEnriched
      .map(joinOutput => {

        val x = Trips.toCsv(joinOutput.right.getOrElse("").asInstanceOf[JoinOutput].left.asInstanceOf[Trips])
        val y = Route.toCsv(joinOutput.right.getOrElse("").asInstanceOf[JoinOutput].right.getOrElse("").asInstanceOf[Route])
        val z = Calendar.toCsv(joinOutput.left.asInstanceOf[Calendar])
        x + "," + y + "," + z
      })

    val outPath = new Path("/user/fall2019/surya/course3/Hadoop_Enriched.txt")
    //fs.create(outPath, true )
    if (fs.exists(outPath)) {
      fs.delete(outPath)
      fs.mkdirs(new Path("/user/fall2019/surya/course3"))
      fs.createNewFile(new Path("/user/fall2019/surya/course3/Hadoop_Enriched.csv"))
      val header = "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date,route_id,service_id,trip_id,trip_headsign,direction_id,shape_id,wheelchair_accessible,note_fr,note_en, route_id,agency_id,route_short_name,route_long_name,route_type,route_url,route_color,route_text_color"

      val outputStream = fs.append(outPath)
      outputStream.writeChars(header + "\n")

      output.foreach(FinalEnriched => outputStream.writeChars(FinalEnriched + "\n"))
      outputStream.close()

    }

    else {
      fs.createNewFile(new Path("/user/fall2019/surya/course3/Hadoop_Enriched.txt"))
      val header = "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date,route_id,service_id,trip_id,trip_headsign,direction_id,shape_id,wheelchair_accessible,note_fr,note_en, route_id,agency_id,route_short_name,route_long_name,route_type,route_url,route_color,route_text_color"

      val outputStream = fs.append(outPath)
      outputStream.writeChars(header + "\n")
      output.foreach(FinalEnriched => outputStream.writeChars(FinalEnriched + "\n"))
      outputStream.close()

    }
    fs.close()
  }
