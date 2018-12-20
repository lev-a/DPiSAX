package fr.inria.zenith

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import play.api.libs.json._

import scala.collection.mutable
import scala.math.sqrt

/**
  * Created by leva on 24/04/2018.
  */



object DPiSAX  {

  val config = AppConfig(ConfigFactory.load())
  def zeroArray =  Array.fill[Int](config.wordLength)(0)


  private def tsToSAX(ts: RDD[(Int, Array[Float])]): RDD[(Array[Int],Int)] = {
    //TODO file -> data
    // normalization
    val tsWithStats = ts.map(t => (t._1, t._2, t._2.sum / t._2.length, t._2.map(x => x * x).sum / t._2.length)).map(t => (t._1, t._2, t._3, sqrt(t._4 - t._3 * t._3).toFloat))
    val tsNorm = tsWithStats.map(t => (t._1, t._2.map(x => (x - t._3) / t._4)))

    //PAA
    val segmentSize = ts.first._2.length / config.wordLength
    val tsSegments = tsNorm.map(ts => (ts._2.sliding(segmentSize, segmentSize).map(t => t.sum / t.length), ts._1))
    tsSegments.map(ts => (ts._1.map(t => config.breakpoints.indexWhere(t <= _)).map(t => if (t == -1) config.breakpoints.length else t).toArray, ts._2))
  }

  def partTreeSplit (tree: SaxNode) : Unit = {
    val partTable = tree.partTable.toList
    val partNode = partTable.maxBy(_._3)._1
    tree.partTreeSplit(partNode)
  }



  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("DPiSAX")
    val sc: SparkContext = new SparkContext(conf)

    //val filename = "/Users/leva/Downloads/ts_1000"
    val tsFilePath = "/Users/leva/GoogleDrive/INRIA/ADT_IMITATES/workspace/sparkDPiSAX/datasets/Seismic_data.data.csv" //TODO parameter
    val financeData = "/Users/leva/GoogleDrive/INRIA/ADT_IMITATES/workspace/sparkDPiSAX/datasets/Finance_Data.csv"

    /*********************************/
    /** Indexing -  Distributed     **/
    /*********************************/
    val inputRDD = sc.textFile(tsFilePath)
                     .map( _.split(',') ).map( t => (t(0).toInt, t.tail.map(_.toFloat) ) )

    val inputFinanceRDD = sc.textFile(financeData)
      .map( _.split(',') ).map( t => (t(0).toInt, t.tail.tail.map(_.toFloat) ) )

    /*********************************/
    /** Building partitioning tree  **/
    /*********************************/
    val numPart = 8 //TODO parameter = number of workers/cores

    /** Sampling **/
    val sampleSize = 0.2 //TODO parameter
    val tsSample = inputRDD.sample(false,sampleSize)
    val sampleToSAX = tsToSAX(tsSample)

    var partTree = new TerminalNode(Array.empty, zeroArray, zeroArray, zeroArray )
    sampleToSAX.collect.foreach{case (saxWord, tsId) => partTree.insert(saxWord, tsId)}
  //  println (partTree.toJSON)
    val partTreeRoot  = partTree.split()
//    println (partTreeRoot.toJSON)

    0 until numPart-2 foreach { _ =>  partTreeSplit(partTreeRoot) }

    /** Partitioning TAble **/
    val partTable = partTreeRoot.partTable
    val partTreeJsonString = partTreeRoot.toJSON
    println (partTreeJsonString)
    //partTable.foreach(println)
    partTable.map(v => (v._1, v._2.mkString("{",",","}"), v._3)).foreach(println)

    def getPartID(tsWord: Array[Int], partTable: Array[((String,Array[Int]),Int)]): Int = {
      def nodeID (saxWord: Array[Int], nodeCard: Array[Int]) = (saxWord zip nodeCard).map {  case (w, c) => (w >> (config.maxCardSymb - c)) + "." + c}.mkString("_")
      partTable.find(v => v._1._1 == nodeID(tsWord, v._1._2)).map(_._2).getOrElse(-1)
    }

    /** Partitioning of input dataset **/
    val partRDD = sc.broadcast(partTable.map(col => (col._1,col._2)).zipWithIndex)
    val inputSAX = tsToSAX(inputRDD)
    val inputSAXpart = inputSAX.map{case (saxWord, tsId) => (getPartID(saxWord, partRDD.value),(saxWord, tsId))}

  //  inputSAXpart.take(20).foreach(println)

    val roots =  inputSAXpart.partitionBy(new HashPartitioner(numPart)).mapPartitions {part =>
      val first =  part.next()
      val root = new InternalNode(partTable(first._1)._2.map(_+1),mutable.HashMap.empty)
      root.insert(first._2._1, first._2._2)
      part.foreach{case (partID, (saxWord, tsId))  => root.insert(saxWord, tsId)}
    // TODO    persist //save JSON to file and termNodes to hdfs + raw ts to the (hdfs)/ or to distributed RDB)
      //TODO update part table with JSON info
       Iterator(root.toJSON) // TODO save to file ' partTable(first._1)._1 + ".json"'
   }
 //    roots.foreach(println)



    /*********************************/
    /**  Distributed   Queryi       **/
    /*********************************/


    /** Parsing partitioning tree from JSON **/
    val json: JsValue = Json.parse(partTreeJsonString) //TODO parameter
    val partTreeDeser = deserJsValue("",json)

    println(partTreeDeser.toJSON)

    val queryFilePath = "/Users/leva/GoogleDrive/INRIA/ADT_IMITATES/workspace/sparkDPiSAX/datasets/seismic_query_10.txt" //TODO parameter
    val queryRDD = sc.textFile(queryFilePath)
                    .map( _.split(',') ).map( t => (t(0).toInt, t.tail.map(_.toFloat) ) )

    val querySAX = tsToSAX(queryRDD)
    val querySAXpart = querySAX.map{case (saxWord, tsId) => (getPartID(saxWord, partRDD.value),(saxWord, tsId))}

/*
    val results =  querySAXpart.partitionBy(new HashPartitioner(numPart)).mapPartitions { part =>
      // TODO parse corresponding index tree from JSON

      // TODO get result with centralized approximate search

    }
*/
  }

  def deserJsValue(jskey: String, jsval: JsValue) : SaxNode = {
    val js = jsval.asInstanceOf[JsObject]
  //  println("_CARD_ = " + js.value("_CARD_").as[String])
    val nodeCard = js.value("_CARD_").as[String].split(",").map(_.toInt)

    if (js.keys.contains("_FILE_")) {
      //println("TN wordToCard = " + jskey.split("_").map(_.split(".").head.toInt).mkString(",")) //TODO parse wordToCard from nodeID, because we need it to create fileName==nodeID or to use  nodeID of childHash as file name, l
      //TODO basically for Querying we need just to check if node isInstance of Terminal and read file
      //println("TN wordToCard =" + jskey.split("_").map(_.split(".")(0).toInt))
      //TODO read tsIDs array from file
      new TerminalNode(Array.empty, nodeCard, zeroArray, Array.empty)
      // create and return terminal node + read ts_list from file
      //TerminalNode (var tsIDs: Array[(Array[Int],Int)], nodeCard: Array[Int], var splitBalance: Array[Int], wordToCard: Array[Int])
    }
    else {
      var childHash  = new mutable.HashMap[String,SaxNode]()
      js.fields.filterNot(_._1 == "_CARD_").foreach( p => childHash += (p._1 -> deserJsValue(p._1, p._2) ) )
      new InternalNode(nodeCard, childHash)
    }
  }
}
