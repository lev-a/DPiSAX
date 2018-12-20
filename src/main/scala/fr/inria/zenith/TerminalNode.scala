package fr.inria.zenith

import com.typesafe.config.ConfigFactory

import scala.collection.mutable

/**
  * Created by leva on 20/07/2018.
  */
class TerminalNode (var tsIDs: Array[(Array[Int],Int)], nodeCard: Array[Int], var splitBalance: Array[Int], wordToCard: Array[Int]) extends SaxNode {

  val config = AppConfig(ConfigFactory.load())
  val nodeID : String  = (wordToCard zip nodeCard).map{case (w,c) => s"$w.$c"}.mkString("_")
  def splitCandList : List[Int] = splitBalance.map(math.abs(_)).zipWithIndex.filter(v => nodeCard(v._2) < config.maxCardSymb).filter(_._1 < tsIDs.length).toList.sortWith(_._1 < _._1).map(_._2)


  override def insert(saxWord: Array[Int] , tsId: Int): Unit  = {
    val wordToCardNext = (saxWord zip nodeCard).map { case (w, c) =>  (w >> (config.maxCardSymb - c - 1) & 0XFF).toByte }
    splitBalance = splitBalance.zip(wordToCardNext).map(v => v._1 + (v._2*2-1))
    //TODO if the cardinality is already max
    tsIDs = tsIDs :+ (saxWord, tsId)
  }

  override def shallSplit : Boolean =  tsIDs.length == config.threshold  && splitCandList.nonEmpty  // and not (cond2) and not (cond3) //  cond2: if all cardinalities nodeCard are already max
  // cond3: if every element of (abs(splitBalance))  == tsIDs.length



  override def split() : SaxNode = {

 //   val splitCandList =   splitBalance.map(math.abs(_)).zipWithIndex.toList.sortWith(_._1 < _._1).map(_._2)
    val elemToSplit = splitCandList.head

    val newNodeCard = nodeCard.updated(elemToSplit, nodeCard(elemToSplit)+1)
 //   println("newNOdeCard" + newNodeCard.mkString(","))
    val newWordToCard = (0 to 1).map(v => wordToCard.updated(elemToSplit, wordToCard(elemToSplit)*2 + v))

    // array of 2 new TerminalNodes with new node Card
    val newTermNodes = newWordToCard.map(v => new TerminalNode(Array.empty, newNodeCard, Array.fill[Int](config.wordLength)(0), v)).toArray

    tsIDs.foreach (ts => newTermNodes(ts._1(elemToSplit) >>  ((config.maxCardSymb - newNodeCard(elemToSplit)) & 0XFF).toByte).insert(ts._1, ts._2) )

    // new InternalNode with hash to two TermNodes (nodeID -> pointer)
    var childHash  = new mutable.HashMap[String,SaxNode]()
    childHash ++= newTermNodes.map(node => node.nodeID -> node )

    new InternalNode(newNodeCard, childHash)


    // (word from list to newCArd) ??? doesnt insert do this + creation of 2 TermNodes (else clause )?
    /*
    val newInternalNode = new InternalNode(newNodeCard, mutable.HashMap.empty)
    tsIDs.foreach(ts => newInternalNode.insert(ts._1, ts._2))// insert on each ts in the List
    newInternalNode
   */
  }

  override def toJSON : String = "{\"_CARD_\" :" + nodeCard.mkString("\"",",","\"") + ", " + "\"_FILE_\" :" + "\"" + nodeID + "\"" +  "}"
//  override def toJSON : String =  "{\"_CARD_\" :" + nodeCard.mkString("\"",",","\"") + ", " + "\"_FILE_\" :" + "\"" + nodeID + "\"" + "_" + tsIDs.length + "}"
//  override def toJSON : String =  "{\"_CARD_\" :" + nodeCard.mkString(",") + ", " + "\"_FILE_\" :" + filename + "[" + tsIDs.map(v =>(v._1.mkString("<",".",">"), v._2)).mkString + "]" + "}"



  override def approximateSearch(saxWord: Array[Int]) : Array[(Array[Int],Int)] = tsIDs

  def partTreeSplit (node: String) : Unit  =  if (node == nodeID) this.split()

  override def partTable  : Array[ (String,Array[Int],Int)] = Array((nodeID, nodeCard,  tsIDs.length))

  //TODO save to file method tsID + word ???

}


