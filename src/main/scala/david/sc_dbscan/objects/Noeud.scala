package david.sc_dbscan.objects


import scala.collection.mutable.HashMap

object NodeBuilder {

  //  --------------------------------------------------------
  //  Create a node from the inputted data
  //  --------------------------------------------------------
  //TODO : devrait être dans la classe Noeud
  /**
    * Creates a node from a string (space separated property names).
    *
    * @param line space separated property names
    * @return the node
    */
  def createNode(line: String, useCoefficient: Boolean): Noeud = {
    var propertString = line.replace("Set", "").replace("(", "").replace(")", "").trim()
    var propertySet = propertString.replaceAll(" +", " ").split(" ")


    //  Get the node's id from the input file
    val noeudId = propertySet.head.toString

    if (!noeudId.equals("")) {
      var finalPropertySet: Set[Int] = Set()

      var coefficient: Int = 1

      if (useCoefficient) {
        try {
          coefficient = propertySet.last.toInt
          propertySet = propertySet.dropRight(1)
        } catch {
          case ioe: NoSuchElementException =>
          //        println(ioe)
          case e: Exception =>
          //        println(e)
        }
      }

      for (i <- 1 to propertySet.length - 1) {
        finalPropertySet = finalPropertySet.+(propertySet(i).toInt)
      }

      //    println(neudId+" - "+finalPropertySet.toSet)

      return new Noeud(noeudId.toInt, finalPropertySet, coefficient)
    }
    else {
      return new Noeud(0, null, 0)
    }
  }



  def createOldNode(line: String): Noeud = {

    var data = line.split(";")

    if(data.size >= 4)
      {
        //  Get the properties of the current node
        var propertyList = data(1).replace("Set(", "").replace(")","").trim().split(" ")
        var properties: Set[Int] = Set()
        propertyList.foreach( p => properties = properties.+(p.replace(",","").replace(" ","").trim().toInt))

        //  Create the node
        var noeud = new Noeud(data(0).toInt, properties, 1)

        //  Set the clusterID
        noeud.setClusted(data(2))

        //  Define number of neighbors
        noeud.setNeighborsNbr(data(3).trim().toInt)

        //  Define whether the node is core
//        if( data(4) == "true" )
//          {
//            noeud.setCore()
//          }

        return noeud
      }

    else return null
  }


  def getOldClusters(line: String): Noeud = {

    var data = line.split(";")

    if(data.size >= 5)
    {
      //  Create the node
      var noeud = new Noeud(data(0).toInt, null, 0)

      //  Set the clusterID
      noeud.setClusted(data(5))

      //  Define whether the node is core
      if( data(4) == "true" )
      {
        noeud.setCore()
      }

      return noeud
    }

    else return null
  }
}



class Noeud(id: Int, properties: Set[Int], coefficient: Int) extends Serializable {

  //The cluster it belongs to
  private var clusterID: String = null

  //  core define whether a node is a core or not
  private var core: Boolean = false

  //  The list of neighbors
  private var neighbors: HashMap[Int, Int] = HashMap()

  //  define whether a nose is visited or not yet during clustering stage
  private var visited: Boolean = false

  //  Define the number of neighbors for a node
  private var neighborsNbr = 0

  //TODO : inutile id est accessible directement
  def getId(): Int = this.id

  //TODO : idem id
  def getProperties(): Set[Int] = this.properties

  def setCore() {
    this.core = true
  }

  def isCore(): Boolean = this.core

  //TODO : définir l'accès à _isCore
  //  def isCore = _isCore
  //  def isCore_= (newValue: Boolean): Unit = { _isCore = newValue }

  //TODO : idem _isCore (_wasVisited)
  def setVisited() {
    this.visited = true
  }

  def setNotVisited() {
    this.visited = false
  }

  def isVisited(): Boolean = this.visited

  def addNeighbor(noaudId: Int, coefficient: Int) {
    this.neighbors.put(noaudId, coefficient)
  }

  //  def addAllNeighbor(noauds: Array[String]) {
  //    this.neighbors = this.neighbors ++ (noauds)
  //  }

  def getNeighbors(): HashMap[Int, Int] = this.neighbors

  def setNeighborList(neighbors: HashMap[Int, Int]) {
    this.neighbors = neighbors
  }

  def getNeighborsNbr(): Int = {
    return this.neighborsNbr
  }

  def setNeighborsNbr(nbr: Int) {
    this.neighborsNbr = nbr
  }

  def getCoefficient(): Int = {

    return this.coefficient
  }

  def getCluster(): String = this.clusterID

  def setClusted(id: String) {
    this.clusterID = id
  }


  //  Override the comparison function
  //  Two nodes are equals if they have the same ID

  def canEqual(a: Any): Boolean = a.isInstanceOf[Noeud]

  override def equals(that: Any): Boolean =
    that match {
      case that: Noeud => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = this.id.hashCode()


  override def toString = s"Noeud($id)"



}
