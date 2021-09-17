package cse512

import java.util
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._


object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)



  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {

    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ", (pickupTime: String) => ((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)


    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)


    // YOU NEED TO CHANGE THIS

    //Count the number of pickup trips
    val count_df = pickupInfo.groupBy("x", "y", "z").count().persist()
    count_df.createOrReplaceTempView("tripscount")

    //Calculate the components of GScore
    var x_bar = pickupInfo.count() / numCells
    var temp_top = spark.sql("select sum(count*count) from tripscount").first().get(0).toString.toDouble
    var sd = math.sqrt(temp_top / numCells - (x_bar * x_bar))

    spark.udf.register("checkNB", (x: Double, y: Double, z: Double, n_x: Double, n_y: Double, n_z: Double) => ((
      HotcellUtils.checkNB(x.toLong, y.toLong, z.toLong, n_x.toLong, n_y.toLong, n_z.toLong)
      )))

    spark.udf.register("getNB", (x: Long, y: Long, z: Long) => ((
      HotcellUtils.getNB(x.toLong, y.toLong, z.toLong)
      )))

    //UDF function to Calculate the Gscore based on given formula
    def getGscore(neighCountReduced: Double, neighCount: Double): Double = {
      var Numerator = neighCountReduced - x_bar * neighCount
      var Denominator = sd * math.sqrt(((numCells * neighCount) - (neighCount * neighCount)) / (numCells - 1))
      var gScore = Numerator / Denominator
      return gScore
    }

    spark.udf.register("getGscore", (neighCountReduced: Double, neighCount: Double) => ((
      getGscore(neighCountReduced, neighCount)
      )))

    var sumofNeighbours = spark.sql("select tc1.x, tc1.y, tc1.z, sum(tc2.count) as neighCountReduced, getNB(tc1.x, tc1.y, tc1.z) as neighCount from tripscount tc1 CROSS JOIN tripscount tc2 where checkNB(tc1.x, tc1.y, tc1.z, tc2.x, tc2.y, tc2.z) group by tc1.x,tc1.y,tc1.z").persist()

    sumofNeighbours.createOrReplaceTempView("sumofNeighbours")

    //Get the list of ZScore sort from high to low
    var getResultList = spark.sql("select x,y,z,getGscore(neighCountReduced, neighCount) as zScore from sumofNeighbours order by zScore desc ")
    getResultList.createOrReplaceTempView("getResultList")

    //Get the list of hot cells as described by x y and z
    var finalResult = spark.sql("select x,y,z from getResultList")


    //Output Final result
    return finalResult


  }

}