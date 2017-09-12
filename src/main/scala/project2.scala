import _root_.akka.actor.Actor
import _root_.akka.actor.ActorRef
import _root_.akka.actor.ActorSystem
import _root_.akka.actor.Props
import akka.actor._
import Array._
import scala.util.Random
import scala.collection.mutable;
import scala.collection.immutable.Queue

// we get some help frome the network to solve the creating grids and push-sum algorithm
// we fix some codes of network to fill in our programs .

object project2 {

  def main(args: Array[String]) {

    var numNodes: Int = args(0).toInt
    var topology: String = args(1).toString
    var algorithm: String = args(2).toString

    val system = ActorSystem("System")
    var AssignMaster = system.actorOf(Props(new GossipMaster(numNodes,topology, algorithm)), "assignMaster");
    AssignMaster ! (numNodes,topology )

  }
}

class GossipMaster(numNodes: Int,topology:String , algorithm: String) extends Actor {

  var nodecount: Int = 0
  val b = System.currentTimeMillis
  //var map = mutable.Map.empty[Int, List[Int]]

  def receive = {
    case (numNodes: Int , topology: String) => {
      // topology match
      topology match {

        case "line" =>   // computeLine grids
          {
           for (i <- 0 to numNodes - 1) {
             if (i == 0)
               space.neighbourmap(i) = List(i + 1)
             else
             if (i == numNodes - 1)
               space.neighbourmap(i) = List(i - 1)
             else
               space.neighbourmap(i) = List(i - 1, i + 1)
             }
          }

        case "full" =>    //compute full grids
          {
            for (i <- 0 to numNodes -1 ) {
                var ls: List[Int] = List()
                for (j <- 0 to numNodes-1 ) {
                  if (i != j)
                   ls = j +: ls
                   }
              space.neighbourmap(i) = ls
              }
           }

        case "3D" =>     //compute 3d grids
        {
          var sqrt: Int = Math.ceil(Math.sqrt(numNodes)).toInt
          for (i: Int <- 0 to numNodes - 1) {
            var ls: List[Int] = List()
            // up neighbour
            if ((i - sqrt) < 0) {
              //ls = (i + (close_sqrt-1)*close_sqrt) +: ls
            }
            else {
              ls = (i - sqrt) +: ls
            }
            // down neighbour
            if ((i + sqrt) > numNodes - 1) {
              //ls = (i - (close_sqrt-1)*close_sqrt) +: ls
            }
            else {
              ls = (i + sqrt) +: ls
            }
            // left neighbour
            if (i % sqrt == 0) {
              //ls = (i + (close_sqrt-1)) +: ls
            }
            else {
              ls = (i - 1) +: ls
            }
            // right neighbour
            if ((i + 1) % sqrt == 0 ) {
              //ls = (i - (close_sqrt-1)) +: ls
            }
            else {
              ls = (i + 1) +: ls
            }
            space.neighbourmap(i) = ls
           }
          }

        case "imp3D" =>    //compute imperfect3d grids
         {
          var sqrt: Int = Math.ceil(Math.sqrt(numNodes)).toInt
          for (i: Int <- 0 to numNodes - 1) {
            var ls: List[Int] = List()
            if ((i - sqrt) < 0) {}
            else {
              ls = (i - sqrt) +: ls
            }
            if ((sqrt + i) > numNodes - 1) {}
            else {
              ls = (sqrt + i) +: ls
            }
            if (i % sqrt == 0) {}
            else {
              ls = (i - 1) +: ls
            }
            if ((i + 1) % sqrt == 0) {}
            else {
              ls = (i + 1) +: ls
            }
            // the extra neighbour
            var ExtraNeighbour = Random.nextInt(numNodes)
            while (ls.contains(ExtraNeighbour)) {
              ExtraNeighbour = Random.nextInt(numNodes)
            }
            ls = ExtraNeighbour +: ls
            space.neighbourmap(i) = ls
           }
          }
      }
      //Creating Actor system and choose the algorithm
      val system2 = ActorSystem("actorSystem")
      var node = Array.ofDim[ActorRef](numNodes)
      for (idNum <- 0 to numNodes -1) {
        var neighbourLen : Int = (space.neighbourmap(idNum)).length
        node(idNum) = system2.actorOf(Props(new Node(idNum, neighbourLen, self)), name = "Node" + idNum)

        algorithm match{
        case "gossip"   => node(0) ! "Hello"
        case "push-sum" => node(0) ! (0.0, 0.0)
        }

      }
    }
    // gossip is done
    case (done: String) => {
      nodecount += 1
      if (nodecount == numNodes) {     // all nodes get the message
        println("Message reached all nodes")
        println("Time taken" + (b-System.currentTimeMillis))
        println("---------End----------")
        //sys.exit();
        context.system.shutdown()
      }
    }
     // push-sum is done
    case (pushDone: String, nodeId: Int, ratio: Double) => {
      nodecount += 1
      if (nodecount == numNodes) {   // all nodes get the message
        println("Message reached all nodes")
        println("Time taken" + (b - System.currentTimeMillis))
        println("----------End----------")
        //sys.exit();
        context.system.shutdown()
      }
    }
  }
}

object space{
  var neighbourmap = mutable.Map.empty[Int, List[Int]];
}

class Node(idNum: Int, neighbourLen: Int, AssignMaster: ActorRef) extends Actor {

  var ratiocount =0
  var messagecount = 0
  var s: Double = idNum
  var w: Double = 1
  var slidingWindow = new scala.collection.mutable.Queue[Double]()
  slidingWindow.enqueue(100)
  slidingWindow.enqueue(100)
  var sign = 0

  def receive = {
    case (rec: String) =>
    {
      //for(count<- 0 to 10){
      if (messagecount < 10) {
        messagecount += 1
        if (messagecount == 10) {
          println("Node" + idNum + " is done!")
          AssignMaster ! "Done"
          //context.stop(self);
        }

        var random: Int = Random.nextInt(neighbourLen)
        var neighbour: Int = space.neighbourmap(idNum).apply(random)
        println("Node"+ idNum +" send message to Node "+ neighbour)
        context.actorSelection("../Node" + neighbour) ! "Hello"
        //println("Node"+ idNum +" send message to Node "+ idNum)
        context.actorSelection("../Node" + idNum) ! "Hello"
      } else {
        var random: Int = Random.nextInt(neighbourLen)
        var neighbour: Int = space.neighbourmap(idNum).apply(random)
        context.actorSelection("../Node" + neighbour) ! "Hello"
        }
      //println("Node" + idNum + " is done!")
      // AssignMaster ! "Done"
      //context.stop(self);
    }


    case (sum: Double, weight: Double) => {
      if (sign == 0) {
        s += sum;
        w += weight;
        var ratio = (s / w)
        s = s / 2;
        w = w / 2;
        slidingWindow.enqueue(ratio)
        if ((Math.abs(slidingWindow.head - slidingWindow.last)) <= 0.001){
          sign = 1;
         // println("sending done to master Node::" + idNum + " ratio::" + ratio)
          AssignMaster ! ("PushDone", idNum, ratio);
        }
        slidingWindow.dequeue
        var random1: Int = Random.nextInt(neighbourLen);
        var neighbour1: Int = space.neighbourmap(idNum).apply(random1);
        println("Node"+ idNum +" send message to Node "+ neighbour1)
        context.actorSelection("../Node" + neighbour1) ! (s, w);
        context.actorSelection("../Node" + idNum) ! (s, w);

      } else {
        var random1: Int = Random.nextInt(neighbourLen);
        var neighbour1: Int = space.neighbourmap(idNum).apply(random1);
        context.actorSelection("../Node" + neighbour1) !(s, w);
      }
    }

  }
}
