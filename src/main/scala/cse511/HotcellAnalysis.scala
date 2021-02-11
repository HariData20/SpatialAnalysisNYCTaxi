package cse511

import cse511.HotcellUtils.{CalculateWeight, CaluclateGScore}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

import scala.math.sqrt

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")


  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)


  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  //Selecting data with in boundaries
  pickupInfo = pickupInfo.select("x","y", "z").where("x >=" +minX+" AND x <="+maxX+" AND y >=" +minY+" AND y <="+maxY+" AND z >=" +minZ+" AND z <="+maxZ)

  var tripsCountData = pickupInfo.groupBy("z", "y", "x").count().withColumnRenamed("count", "trips_measure").orderBy("trips_measure")
  tripsCountData.createOrReplaceTempView("totaltripsCountData")

  val mean = tripsCountData.select("trips_measure").agg(sum("trips_measure")).first().getLong(0) / numCells

  val stdev = sqrt((tripsCountData.withColumn("squared",pow(col("trips_measure"),2) ).select("squared").agg(sum("squared")).first().getDouble(0)/ numCells) - scala.math.pow(mean, 2) )

  var neighboursData = spark.sql("select t1.x, t1.y, t1.z, sum(t2.trips_measure) as neighboursTrips from totaltripsCountData t1,totaltripsCountData t2 " +
    "where (t2.x == t1.x OR t2.x == t1.x-1 OR t2.x== t1.x+1)" +
    "AND (t2.y == t1.y OR t2.y == t1.y-1 OR t2.y== t1.y+1)" +
    "AND (t2.z == t1.z OR t2.z == t1.z-1 OR t2.z== t1.z+1) " +
    "GROUP BY t1.x,t1.y,t1.z")
  //UDF currying inorder to pass parameters and column values.
  val forWeightFunction=udf((minX: Double, minY: Double, minZ: Double, maxX: Double, maxY: Double, maxZ: Double, X: Double, Y: Double, Z: Double) => CalculateWeight(minX, minY, minZ, maxX, maxY, maxZ, X, Y, Z))
  neighboursData = neighboursData.withColumn("SpatialWeight", forWeightFunction(lit(minX), lit(minY), lit(minZ), lit(maxX), lit(maxY), lit(maxZ), col("x"), col("y"), col("z")))

  val forGScoreFunc = udf((numCells: Double ,SpatialWeight: Int, neighboursTrips: Int, avg: Double, stdDev: Double) => CaluclateGScore(numCells, SpatialWeight, neighboursTrips, avg, stdDev))
  neighboursData = neighboursData.withColumn("GScore", forGScoreFunc(lit(numCells),  col("SpatialWeight"), col("neighboursTrips"), lit(mean), lit(stdev))).orderBy(desc("GScore"))

  pickupInfo = neighboursData.select(col("x"), col("y"), col("z"))
  return pickupInfo
}
}
