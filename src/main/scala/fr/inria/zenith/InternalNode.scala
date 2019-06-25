package fr.inria.zenith

import com.typesafe.config.ConfigFactory

import scala.collection.mutable


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

  override def approximateSearch(saxWord: Array[Int], paa: Array[Float]) : Array[Long]  = {
    val nodeID : String  = (wordToCard(saxWord) zip childCard).map{case (w,c) => s"$w.$c"}.mkString("_")

    var result : Array[Long] = if (childHash.contains(nodeID))
      childHash(nodeID).approximateSearch(saxWord, paa)
    else
      Array.empty

    if (result.length < config.topk) {
      childHash.filter(_._1 != nodeID)
        .map{ case(nodeId, node) => (config.mindist(paa, node.wordToCard, node.nodeCard, 1), node) }
        .toArray.sortBy(_._1).map(_._2)
        .takeWhile { node =>
          result ++= node.approximateSearch(saxWord, paa)
          result.length < config.topk
        }
    }

    result
  }

  override def boundedSearch(paa: Array[Float], bound: Float, tsLength: Int): Array[Long] =
    childHash
      .filter(c => config.mindist(paa, c._2.wordToCard, c._2.nodeCard, tsLength) <= bound)
      .flatMap( _._2.boundedSearch(paa, bound, tsLength) ).toArray

  def fullSearch : Array[Long] = childHash.flatMap(_._2.fullSearch).toArray

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



