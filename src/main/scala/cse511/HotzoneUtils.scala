package cse511

import scala.math.{max, min}

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    if(queryRectangle == null || pointString == null ){
      return false
    }
    if(queryRectangle.split(',').length != 4 || pointString.split(',').length != 2){
      return false
    }
    var rectangleCoordinates = queryRectangle.split(',')
    var pointCoordinates = pointString.split(',')

    //Reading given Strings to co-ordinates
    var x1 = min(rectangleCoordinates(0).toDouble,rectangleCoordinates(2).toDouble)
    var y1 = min(rectangleCoordinates(1).toDouble,rectangleCoordinates(3).toDouble)
    var x2 = max(rectangleCoordinates(0).toDouble,rectangleCoordinates(2).toDouble)
    var y2 = max(rectangleCoordinates(1).toDouble,rectangleCoordinates(3).toDouble)

    var x = pointCoordinates(0).toDouble
    var y = pointCoordinates(1).toDouble

    //Checking for condition if (x,y) lies in between (x1,y1) and (x2,y2) and x1< x2 and y1 < y2
    if(x >= x1 && x <= x2 && y >= y1 && y <= y2 ){
      return true
    }
    else {
      return false
    }
  }

  // YOU NEED TO CHANGE THIS PART

}
