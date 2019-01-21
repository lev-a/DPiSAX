package fr.inria.zenith

import com.typesafe.config.ConfigFactory

import scala.collection.mutable

/**
  * Created by leva on 20/07/2018.
  */
class InternalNode (nodeCard /* card */: Array[Int], childHash: mutable.HashMap[String /* word_to_card.card = nodeID */, SaxNode]  ) extends SaxNode {

  val config = AppConfig(ConfigFactory.load()) //is it good to define in class ? how is better to made available global configuration in class objects ?


  private def wordToCard (saxWord: Array[Int]) = (saxWord zip nodeCard).map { case (w, c) => w >> (config.maxCardSymb - c) }

  override def insert(saxWord: Array[Int] , tsId: Int ): Unit  = {

    //val word = wordToCard(saxWord).mkString("\"",",","\"")
    val nodeID : String  = (wordToCard(saxWord) zip nodeCard).map{case (w,c) => s"$w.$c"}.mkString("_")

    if (childHash.contains(nodeID)){

      var child = childHash(nodeID)
      if (child.isInstanceOf[TerminalNode] && child.shallSplit) {
        child = child.split()
        childHash(nodeID) = child
      }
      child.insert(saxWord, tsId)
    }
    else { // when TerminalNode doesn't exist
      var newTermNode = new TerminalNode(Array.empty, nodeCard, Array.fill[Int](config.wordLength)(0), wordToCard(saxWord))
      this.childHash += nodeID -> newTermNode
      newTermNode.insert(saxWord, tsId)
    }

  }

  override def shallSplit : Boolean =  false

  override def split () : InternalNode  = this

  override def toJSON : String =  "{\"_CARD_\" :" + nodeCard.mkString("\"",",","\"") + ", " + childHash.map(child => child._1.mkString("\"","","\"") + ":" + child._2.toJSON ).mkString(",") + "}"

  override def approximateSearch(saxWord: Array[Int]) : Array[(Array[Int],Int)]  = {

   // val word = wordToCard(saxWord).mkString("\"",",","\"")
    val nodeID : String  = (wordToCard(saxWord) zip nodeCard).map{case (w,c) => s"$w.$c"}.mkString("_")
/*
    try{
      childHash(word).approximateSearch(saxWord)
    } catch {
      case e: Exception => println("exception caught:" + e)
    }
    */

    if (childHash.contains(nodeID)) //needed only for root node
   {
      childHash(nodeID).approximateSearch(saxWord)}
    else Array.empty
     //TODO key not found exception
  }

  def partTreeSplit (nodeToSplitID: String) : Unit = {

    if (childHash.contains(nodeToSplitID) && childHash(nodeToSplitID).isInstanceOf[TerminalNode] ){
      var child = childHash(nodeToSplitID)
      child = child.split()
      childHash(nodeToSplitID) = child
   //   childHash(nodeToSplitID).split(nodeCard)
      }
    else childHash.foreach(_._2.partTreeSplit(nodeToSplitID))
  }

  override def partTable  : Array[(String,Array[Int],Int)] = childHash.flatMap(_._2.partTable).toArray
}



