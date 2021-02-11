package cse511

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.math.sqrt

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int = {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(", "").toDouble / coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")", "").toDouble / coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser(timestampString: String): Timestamp = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth(timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def CalculateWeight(minX: Double, minY: Double, minZ: Double, maxX: Double, maxY: Double, maxZ: Double, X: Double, Y: Double, Z: Double): Int = {
    var count = 0
    // Point on X-coordinate
    if (X == minX || X == maxX) {
      count += 1
    }
    // Point on X-coordinate and Y-coordinate
    if (Y == minY || Y == maxY) {
      count += 1
    }
    // Point on X-coordinate, Y-coordinate, and Z-coordinate
    if (Z == minZ || Z == maxZ) {
      count += 1
    }
    count match {
      case 1 => 18 //Face cube
      case 2 => 12 //Edge cubes
      case 3 => 8 //Corner cubes
      case _ => 27 //Inner cubes
    }
  }

  def CaluclateGScore(numCells: Double, SpatialWeight: Int, neighboursTrips: Int, mean: Double, stdev: Double): Double = {
    val gscore = (neighboursTrips - (mean * SpatialWeight)) / (stdev * sqrt(((numCells * SpatialWeight) - (SpatialWeight * SpatialWeight)) / (numCells - 1)))
    return gscore
    // YOU NEED TO CHANGE THIS PART
  }
}