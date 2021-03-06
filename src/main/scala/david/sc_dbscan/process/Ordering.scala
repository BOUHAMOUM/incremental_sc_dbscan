package david.sc_dbscan.process


import org.apache.spark.rdd.RDD


object Ordering {

  //  ----------------------------------------------------------
  //  Read the order of properties from the old clustering process
  //  ----------------------------------------------------------
  def getPropertyOrder(files: RDD[String]): Map[Int, Int] = {

    val properties = files.map {
      line =>

        var propertyList = line.split(";")

        (propertyList(0).toInt, propertyList(1).toInt)
    }

    return properties.collect().toMap
  }


  //  ----------------------------------------------------------
  //  Generate the first partitions according to the propertySet
  //  ----------------------------------------------------------
  def getPropertyOrder(files: RDD[String], coefficient: Boolean): Map[Int, Int] = {

    val properties = files.flatMap {
      line =>

        var propertyList = line.split(" ")

        var propetySize = propertyList.length - 1

        //        If the last column is a coefficient, it won't be considered
        if (coefficient) {
          propetySize = propetySize - 1
        }

        for (i <- 1 to propetySize; if propertyList(i) != "") yield {
          (propertyList(i).toInt, 1)
        }
    }.reduceByKey(_ + _)

    return properties.collect().toMap
  }
}
