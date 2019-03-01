package fr.inria.zenith

import com.typesafe.config.ConfigFactory

import scala.collection.mutable

/**
  * Created by leva on 20/07/2018.
  */
class InternalNode (childCard: Array[Int], childHash: mutable.HashMap[String /* word_to_card.card = nodeID */, SaxNode], nodeCard: Array[Int], wordToCard: Array[Int]  ) extends SaxNode(nodeCard, wordToCard) {

  val config = AppConfig(ConfigFactory.load())

  private def wordToCard (saxWord: Array[Int]) = (saxWord zip childCard).map { case (w, c) => w >> (config.maxCardSymb - c) }

  override def insert(saxWord: Array[Int] , tsId: Long ): Unit  = {
    val nodeID : String  = (wordToCard(saxWord) zip childCard).map{case (w,c) => s"$w.$c"}.mkString("_")

    if (childHash.contains(nodeID)){
      var child = childHash(nodeID)
      if (child.isInstanceOf[TerminalNode] && child.shallSplit) {
        child = child.split()
        childHash(nodeID) = child
      }
      child.insert(saxWord, tsId)
    }
    else {
      val newTermNode = new TerminalNode(Array.empty, childCard, wordToCard(saxWord))
      this.childHash += nodeID -> newTermNode
      newTermNode.insert(saxWord, tsId)
    }
  }

  override def shallSplit : Boolean =  false

  override def split () : InternalNode  = this

  override def toJSON (fsURI: String) : String =  "{\"_CARD_\" :" + childCard.mkString("\"",",","\"") + ", " + childHash.map(child => child._1.mkString("\"","","\"") + ":" + child._2.toJSON(fsURI) ).mkString(",") + "}"

  override def approximateSearch(saxWord: Array[Int], paa: Array[Float]) : Array[(Array[Int], Long)]  = {
    val nodeID : String  = (wordToCard(saxWord) zip childCard).map{case (w,c) => s"$w.$c"}.mkString("_")

    if (childHash.contains(nodeID)) {
      childHash(nodeID).approximateSearch(saxWord, paa)
     }
    else {
      childHash.map{ case(nodeId, node) => (config.mindist(paa, node.wordToCard, node.nodeCard, 1), node) }.toArray.minBy(_._1)._2.approximateSearch(saxWord, paa)
    }
/*
    else if (childHash.size == 1) {
      childHash.head._2.fullSearch
    }
    else Array.empty
*/
  }

  override def boundedSearch(paa: Array[Float], bound: Float, tsLength: Int): Array[(Array[Int], Long)] =
    childHash
      .filter(c => config.mindist(paa, c._2.wordToCard, c._2.nodeCard, tsLength) <= bound)
      .flatMap( _._2.boundedSearch(paa, bound, tsLength) ).toArray

  override def boundedSearch(qs: Array[(Long, (Array[Float], Float, Int))]) : Array[(Long, Long)] =
    childHash.map( _._2.boundedSearch(qs) ).reduce(_++_)

  def fullSearch : Array[(Array[Int],Long)] = childHash.flatMap(_._2.fullSearch).toArray

  def partTreeSplit (nodeToSplitID: String) : Unit = {

    if (childHash.contains(nodeToSplitID) && childHash(nodeToSplitID).isInstanceOf[TerminalNode] ){
      var child = childHash(nodeToSplitID)
      child = child.split()
      childHash(nodeToSplitID) = child
      }
    else childHash.foreach(_._2.partTreeSplit(nodeToSplitID))
  }

  override def partTable  : Array[(String,Array[Int],Int)] = childHash.flatMap(_._2.partTable).toArray
}



