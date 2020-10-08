package david.sc_dbscan.process

import david.sc_dbscan.objects.Noeud

object Compare {
  
  def similar(n1: Noeud, n2: Noeud, eps:Double): Boolean = {
    
    var intersection:Double = n1.getProperties().toSet.intersect(n2.getProperties().toSet).size
    var union:Double = n1.getProperties().toSet.union(n2.getProperties().toSet).size
    
    var cpt:Double = intersection / union

//    val x = List(74362, 74695, 74857, 73983)
//    val y = List(64974, 62712, 57656, 63400, 60558, 66527, 64301)
//
//    if( x.contains(n1.getId()) && y.contains(n2.getId()))
//      println(n1+" "+n2+" = "+cpt)
//        println(n1+" "+n2+" = "+cpt)

    if( cpt >= eps )
      return true
    else return false
  }

  def similar(n1: Noeud, n2: Set[Int], eps:Double): Boolean = {

    var intersection:Double = n1.getProperties().toSet.intersect(n2).size
    var union:Double = n1.getProperties().toSet.union(n2).size

    var cpt:Double = intersection / union

    //    println(n1+" "+n2+" = "+cpt)

    if( cpt >= eps )
      return true
    else return false
  }
}