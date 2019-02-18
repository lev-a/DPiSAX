package fr.inria.zenith

import com.typesafe.config.ConfigFactory

import scala.collection.mutable

/**
  * Created by leva on 20/07/2018.
  */
class TerminalNode (var tsIDs: Array[(Array[Int],Long)], nodeCard: Array[Int], var splitBalance: Array[Array[Int]], wordToCard: Array[Int]) extends SaxNode {

  val config = AppConfig(ConfigFactory.load())
  val nodeID : String = config.nodeID(wordToCard, nodeCard)
  var maxCardStep = config.maxCardSymb

  def splitCandList : List[(Int,Int,Int)] = splitBalance.map(_.map(math.abs(_)).zipWithIndex.dropWhile(_._1 >= tsIDs.length)).zipWithIndex.filter(_._1.nonEmpty).map(v => (v._1.head, v._2)).filter(v => nodeCard(v._2) < config.maxCardSymb).toList.sortBy(r => r._1._2 * tsIDs.length + r._1._1).map(v => (v._2,v._1._2,v._1._1)).filter(_._2 < maxCardStep)
  // Array[(elem_to_split_position, cardinality_to_split_on)]



  override def insert(saxWord: Array[Int] , tsId: Long): Unit  = {
    val wordToCardNext = (saxWord zip nodeCard).map { case (w, c) => for (i <- c until config.maxCardSymb) yield { (w >> (config.maxCardSymb - i - 1) & 0XFF).toByte}  }
    splitBalance = splitBalance.zip(wordToCardNext).map(v => v._1.zip(v._2).map(v => v._1 + ((v._2 % 2) * 2 - 1)))
    //TODO if the cardinality is already max
    tsIDs = tsIDs :+ (saxWord, tsId)
  }

  override def shallSplit : Boolean =  tsIDs.length >= config.threshold  && splitCandList.nonEmpty

  override def split() : SaxNode = {

    val elemToSplit  = splitCandList.head

    val cardStep = elemToSplit._2 + 1

    val dw = splitBalance(elemToSplit._1).take(elemToSplit._2).map(v => if (v>0) 1 else 0).reverse.zipWithIndex.map(v => v._1 << v._2).sum

    val newNodeCard = nodeCard.updated(elemToSplit._1, nodeCard(elemToSplit._1) + cardStep)
    val newWordToCard = (0 to 1).map(v => wordToCard.updated(elemToSplit._1, ((wordToCard(elemToSplit._1) << (cardStep-1)) + dw) * 2 + v))
    val newTermNodes = newWordToCard.map(v => new TerminalNode(Array.empty, newNodeCard, config.basicSplitBalance(newNodeCard), v)).toArray

    tsIDs.foreach(ts => newTermNodes((ts._1(elemToSplit._1) >> ((config.maxCardSymb - newNodeCard(elemToSplit._1)) & 0XFF).toByte) % 2).insert(ts._1, ts._2))

    var childHash = new mutable.HashMap[String, SaxNode]()
    childHash ++= newTermNodes.map(node => node.nodeID -> node)

    var newInternalNode = new InternalNode(newNodeCard, childHash)

    for (i <- 1 until cardStep) {
         val newIntNodeCard = newNodeCard.updated(elemToSplit._1, newNodeCard(elemToSplit._1) - i)
         val newIntWordToCard = newWordToCard(0).updated(elemToSplit._1,newWordToCard(0)(elemToSplit._1) >> i)
         val newNodeID  = config.nodeID(newIntWordToCard, newIntNodeCard)
         childHash =  new mutable.HashMap[String, SaxNode]()
         childHash += newNodeID -> newInternalNode
         newInternalNode = new InternalNode(newIntNodeCard, childHash)
    }

    newInternalNode
  }

  override def toJSON (fsURI: String) : String = {
    tsToFile(fsURI) //TODO where should be this call ?
    "{\"_CARD_\" :" + nodeCard.mkString("\"", ",", "\"") + ", " + "\"_FILE_\" :" + "\"" + nodeID + "\"" + ", \"_NUM_\":" + tsIDs.length + "}"
  }

  override def approximateSearch(saxWord: Array[Int]) : Array[(Array[Int], Long)] = tsIDs

  override def boundedSearch(paa: Array[Float], bound: Float, tsLength: Int): Array[(Array[Int], Long)] = {
    if (config.mindist(paa, wordToCard, nodeCard, tsLength) <= bound)
      tsIDs.filter( t => config.mindist(paa, t._1, Array.fill[Int](config.wordLength)(config.maxCardSymb), tsLength) <= bound )
    else
      Array.empty
  }

  def fullSearch :  Array[(Array[Int], Long)] = tsIDs

  def partTreeSplit (node: String) : Unit  =  if (node == nodeID) this.split()

//  override def partTable  : Array[ (String,Array[Int],Int)] = Array((nodeID, nodeCard,  tsIDs.length ))
  override def partTable  : Array[ (String,Array[Int],Int)] = {
    maxCardStep = 1
    Array((nodeID, nodeCard,  (tsIDs.length - splitCandList.map(_._3).headOption.getOrElse(tsIDs.length))/2))
  }


  def tsToFile (fsURI: String) =  {
   val writer = DPiSAX.setWriter(fsURI, config.workDir + nodeID)
     tsIDs.foreach(t => writer.write (t._1.mkString(",") + " " + t._2 + "\n") )
     writer.close
  } //TODO close fs ????


}


