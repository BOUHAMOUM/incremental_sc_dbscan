package david.sc_dbscan.process

import org.apache.spark.rdd.RDD
import david.sc_dbscan.objects.Noeud
import scala.collection.mutable.HashMap

object CoresIdentification {

  def coresIdentification(partitions: RDD[(Set[Int], Array[Noeud])], minPts: Int, eps: Double): RDD[(Set[Int], Array[Noeud])] = {

    //  Structure that assign to each node the partition it belongs to and its neighbors
    case class NeighborsByPartition(partitions: Array[Set[Int]], neighbors: HashMap[Int, Int])

    val partialNeighbors: RDD[(Noeud, NeighborsByPartition)] = partitions.flatMap {

      part =>
        var result: Array[Noeud] = Array()
        var partitionId = part._1

        if( part._2.size > 2 )
         {
           //  Cartesian product to find out neighbors for each node
           part._2.foreach { currentNoeud =>

             if(currentNoeud.getCluster() == null)
             {
               part._2.foreach { noeud =>

                 if (noeud != currentNoeud && Compare.similar(currentNoeud, noeud, eps)) {
                   currentNoeud.addNeighbor(noeud.getId(), noeud.getCoefficient())

                   //  update the list of neighbor for old entities to found out those taht become cores
                   if( noeud.getCluster() != null )
                   {
                     noeud.addNeighbor(currentNoeud.getId(), currentNoeud.getCoefficient())

                     //                    result = result :+ noeud
                   }
                 }
               }

               //                if( currentNoeud.getNeighbors().size > 0)
               //                  result = result :+ currentNoeud
             }
           }


           part._2.foreach{
             noeud =>
               if( noeud.getNeighbors().size > 0 )
                 result = result :+ noeud
           }
         }

        for (i <- 0 to result.length - 1) yield {
          //  for each node, return the partition it belongs to and its neighbors in the current partition
          val neighborsPartition = new NeighborsByPartition(Array(partitionId.toSet), result(i).getNeighbors())
          (result(i), neighborsPartition)
        }
    }

    //  for each node, group its neighbors finded out throw the different partition
    val totalNeighbors: RDD[(Noeud, NeighborsByPartition)] = partialNeighbors.reduceByKey { (x, y) =>

      var neighborsList = x.neighbors.++:(y.neighbors)
      var partitions = x.partitions.++:(y.partitions)

      new NeighborsByPartition(partitions.distinct, neighborsList)
    }

      //  repartition the data according to the partitionID
      val newNoeuds = totalNeighbors.flatMap { noeud =>

      val newNoeud = new Noeud(noeud._1.getId(), null, noeud._1.getCoefficient())
      newNoeud.setNeighborsNbr( noeud._1.getNeighborsNbr() )

      //  We keep only the core, since they contain even the neighbors (the cores have all the information to build the clusters)
      if (noeud._2.neighbors.values.sum + newNoeud.getCoefficient() + newNoeud.getNeighborsNbr() - 1 >= minPts)
      {
        newNoeud.setCore()
        newNoeud.setNeighborList(noeud._2.neighbors)
      }
        

        var partitions = noeud._2.partitions
        var partitionsSize = partitions.length - 1


      for (i <- 0 to partitionsSize) yield {
        (partitions(i), Array(newNoeud))
      }
    }

    //  Return for each partition, the list of nodes
    val updatedNoeuds = newNoeuds.reduceByKey { (x, y) => x.++:(y) }

    return updatedNoeuds
  }
}