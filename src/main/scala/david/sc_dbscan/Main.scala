package david.sc_dbscan

import java.io.{File, PrintWriter}

import david.sc_dbscan.objects.Noeud
import david.sc_dbscan.process.{Clustring, CoresIdentification, GlobalMerging, Ordering, Partitioning}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

import scala.reflect.ClassTag


object Main {

  val logger = Logger.getLogger(getClass.getName)

  val usage =
    """
      |Usage: sc_dbscan --eps e --mpts m --cap c file
      |    --eps e  : threshold for clustering
      |    --mpts p : minimum number of points
      |    --cap c  : capacity of a node
      |    --coef cf: coefficient is associated to patterns
      |    file     : data filename
    """.stripMargin

  type OptionMap = Map[Symbol, Any]

  def parseArguments(map: OptionMap, arguments: List[String]): OptionMap = {
    arguments match {
      case Nil => map
      case "--eps" :: value :: tail =>
        parseArguments(map ++ Map('eps -> value.toDouble), tail)
      case "--mpts" :: value :: tail =>
        parseArguments(map ++ Map('mpts -> value.toInt), tail)
      case "--mergeloop" :: value :: tail =>
        parseArguments(map ++ Map('mergeloop -> value.toInt), tail)
      case "--cap" :: value :: tail =>
        parseArguments(map ++ Map('cap -> value.toInt), tail)
      case "--coef" :: value :: tail =>
        parseArguments(map ++ Map('coef -> value.toBoolean), tail)
      case "--oldData" :: value :: tail=>
        parseArguments(map ++ Map('oldData -> value.toString()), tail)
      case "--propertyOrder" :: value :: tail =>
        parseArguments(map ++ Map('propertyOrder -> value.toString()), tail)
      case dataFile :: Nil =>
        map ++ Map('datafile -> dataFile)
      case option :: tail =>
        println(usage)
        throw new IllegalArgumentException(s"Unknown argument $option");
    }
  }


  //  val conf = new SparkConf()
  //    .setAppName("SC_DBSCAN")
  ////    .setMaster("local[*]")
  //    .setMaster("spark://s0.adam.uvsq.fr:7077")
  //
  //  val sc = new SparkContext(conf)


  //  val propertyCodification: HashMap[String, Int] = HashMap()
  //
  //  val propertyDecoding: HashMap[Int, String] = HashMap()

  //  val corpsNumber = sc.accumulator(0)

  //    Add a parameter to determine whethier we use coefficients or not
  //  var useCoefficient = false

  //  var temporaryEps: Double = 0

  //  --------------------------------------------------------
  //  Change the properties by codes
  //  --------------------------------------------------------
  //  def codifiate(prop: Set[String]): Set[Int] = {
  //
  //    var newProp: Set[Int] = Set()
  //
  ////    prop.foreach { p =>
  ////      newProp = newProp + propertyCodification.get(p).get
  ////    }
  //
  //    prop.foreach { p =>
  //      newProp = newProp + p.toInt
  //    }
  //
  //    return newProp
  //  }

  //  --------------------------------------------------------
  //  Bring back the properties to the initial value
  //  --------------------------------------------------------
  //  def decoding(prop: Set[Int]): Set[String] = {
  //
  //    var initialProp: Set[String] = Set()
  //
  ////    prop.foreach { p =>
  ////      initialProp = initialProp + propertyDecoding.get(p).get.toString()
  ////    }
  //
  //
  //    prop.foreach { p =>
  //      initialProp = initialProp + p.toString()
  //    }
  //
  //    return initialProp
  //  }

  def main(args: Array[String]): Unit = {

    println("START_V2.3_MERGING")

    logger.debug(s"Start of main method with arguments ${args.mkString(" ")}")
    val configuration = parseArguments(Map(), args.toList)


    val eps = configuration.getOrElse('eps, 0.5).asInstanceOf[Double]
    val minPts = configuration.getOrElse('mpts, 2).asInstanceOf[Int]
    val nodeCapacity = configuration.getOrElse('cap, 15000).asInstanceOf[Int]
    val coefficient = configuration.getOrElse('coef, false).asInstanceOf[Boolean]
    val dataFile = configuration.getOrElse('datafile, "/home/red/Bureau/incremental-dbscan/SC_DBSCAN-savingDataForIncr(text)/data/data.txt").asInstanceOf[String]

    // Get information about the old data
    val oldClusters = configuration.getOrElse('oldData, "/home/red/Bureau/incremental-dbscan/SC_DBSCAN-savingDataForIncr(text)/data/oldData.txt").asInstanceOf[String]

    // Number of loop during the merging step
    val mergeLoop = configuration.getOrElse('mergeloop, 5).asInstanceOf[Int]

    logger.debug(s"Parsed parameters : eps = $eps, minPts = $minPts, capacity = $nodeCapacity, coefficient = $coefficient data file = $dataFile, merging loop = $mergeLoop")

    val sparkSession = SparkSession.builder
//      .master("local")
      .appName("inc-SC_DBSCAN(readingTXT): "+dataFile)
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val data = sc.textFile(dataFile)
    val oldData = sc.textFile(oldClusters)

    //  Order the properties of new entities
    val r1 = Ordering.getPropertyOrder(data, coefficient)

    //  Distribute new entities
    val newData = Partitioning.getInitalPartition(data, eps, coefficient, r1)

    //  Get the list of created partitions
    val partitionsList = newData.keys.collect()

    //  Distribute new entities
    val oData = Partitioning.getOldPartition(oldData, eps, coefficient, r1, partitionsList)

    //  Group new entities with old ones
    val totalData = newData.union(oData).reduceByKey(_ ++ _)

    val r3 = Partitioning.getFinalPartitions(totalData, nodeCapacity, eps, r1)

    val newElements = newData.map{
      elem => (elem._1, elem._2.size)
    }.reduceByKey(_+_)

    val oldElements = oData.map{
      elem => (elem._1, elem._2.size)
    }.reduceByKey(_+_)

    val nbrElements = r3.map{
      elem => (elem._1, elem._2.size)
    }.reduceByKey(_+_)

    println("New Elements in Partitions : " + newElements.values.sum()+" MIN "+newElements.values.min()+" MAX "+newElements.values.max())
    println("Old Elements in Partitions : " + oldElements.values.sum()+" MIN "+oldElements.values.min()+" MAX "+oldElements.values.max())
    println("Totals Elements in Partitions : " + nbrElements.values.sum()+" MIN "+nbrElements.values.min()+" MAX "+nbrElements.values.max())
    println("Final Partitions : " + r3.count())


    val r4 = CoresIdentification.coresIdentification(r3, minPts, eps)

    val r5 = Clustring.generateClusters(r4)

    val r6 = GlobalMerging.clustersMerging(r5, mergeLoop)

//    println("Final Partitions : " + r3.count())
    println("Clusters : " + r6.size)


//    val writer=new PrintWriter(new File(dataFile+"_clustersINC.txt"))

//    println("---------------Final Clusters--------------")
//    var i = 1
//    r6.foreach { f =>
//      var x = "C" + i + " : " + f._1.toString() + " : "
//
//
//      var cl = 0
//
//      cl = i
//      i = i + 1
//
//
//      f._2.foreach { p =>
//        x = x + p.getId().toString+"; "
//      }
//
//      print(x+"\n")
//
//    }

//    writer.close()


    sc.stop()
    sparkSession.stop()

    println("END")
    logger.debug("End of main method")
  }

}
