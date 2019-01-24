package fr.inria.zenith

import java.io.PrintWriter

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import play.api.libs.json._

import scala.collection.mutable
import scala.io.Source
import scala.math.sqrt

/**
  * Created by leva on 24/04/2018.
  */



object DPiSAX  {

  val config = AppConfig(ConfigFactory.load())
  def zeroArray =  Array.fill[Int](config.wordLength)(0)


  private def tsToSAX(ts: RDD[(Int, Array[Float])]): RDD[(Array[Int],Int)] = {
    // normalization
    val tsWithStats = ts.map(t => (t._1, t._2, t._2.sum / t._2.length, t._2.map(x => x * x).sum / t._2.length)).map(t => (t._1, t._2, t._3, sqrt(t._4 - t._3 * t._3).toFloat))
    val tsNorm = tsWithStats.map(t => (t._1, t._2.map(x => (x - t._3) / t._4)))

    //PAA
    val tsLength = ts.first._2.length
    val segmentSize = tsLength / config.wordLength
    val numExtraSegments = tsLength % config.wordLength
    val sliceBorder = (config.wordLength - numExtraSegments)*segmentSize
    val tsSegments = tsNorm.map(ts => ((ts._2.slice(0,sliceBorder).sliding(segmentSize, segmentSize) ++ ts._2.slice(sliceBorder,tsLength).sliding(segmentSize+1, segmentSize+1)).map(t => t.sum / t.length), ts._1))
    //val tsSegments = tsNorm.map(ts => (ts._2.sliding(segmentSize, segmentSize).map(t => t.sum / t.length), ts._1))
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

    //val tsFilePath = "/Users/leva/Downloads/ts_1000"
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

    var partTree = new TerminalNode(Array.empty, zeroArray, config.basicSplitBalance(zeroArray), zeroArray )
    sampleToSAX.collect.foreach{case (saxWord, tsId) => partTree.insert(saxWord, tsId)}
  //  println (partTree.toJSON)
    val partTreeRoot  = partTree.split()
//    println (partTreeRoot.toJSON)

    0 until numPart-2 foreach { _ =>  partTreeSplit(partTreeRoot) }

    /** Partitioning TAble **/
    val partTable = partTreeRoot.partTable
    val partTreeJsonString = partTreeRoot.toJSON //TODO save to file
    println ("partTreeJsonString = " + partTreeJsonString)
    //partTable.foreach(println)
   // partTable.map(v => (v._1, v._2.mkString("{",",","}"), v._3)).foreach(println)

    def getPartID(tsWord: Array[Int], partTable: Array[((String,Array[Int]),Int)]): Int = {
      def nodeID (saxWord: Array[Int], nodeCard: Array[Int]) = (saxWord zip nodeCard).map {  case (w, c) => (w >> (config.maxCardSymb - c)) + "." + c}.mkString("_")
      partTable.find(v => v._1._1 == nodeID(tsWord, v._1._2)).map(_._2).getOrElse(-1)
    }

    /** Partitioning of input dataset **/
    val partRDD = sc.broadcast(partTable.map(col => (col._1,col._2)).zipWithIndex)
    val inputSAX = tsToSAX(inputRDD)
    val inputSAXpart = inputSAX.map{case (saxWord, tsId) => (getPartID(saxWord, partRDD.value),(saxWord, tsId))}

  //  inputSAXpart.take(20).foreach(println)

    val roots =  inputSAXpart.partitionBy(new HashPartitioner(numPart)).foreachPartition {part =>
      val first =  part.next()
      val root = new InternalNode(partTable(first._1)._2.map(_+1),mutable.HashMap.empty)
      root.insert(first._2._1, first._2._2)
      part.foreach{case (partID, (saxWord, tsId))  => root.insert(saxWord, tsId)}
    // TODO    persist //save JSON to file and termNodes to hdfs + raw ts to the (hdfs)/ or to distributed RDB) --> decided not to redistribute
      //TODO update part table with JSON info ??? --> no need
     // save to file ' partTable(first._1)._1 + ".json"'
      new PrintWriter(partTable(first._1)._1 + ".json") { write(root.toJSON); close } //TODO path to working dir
      Iterator(root.toJSON)
   }
  // roots.foreach(println)



    /*********************************/
    /**  Distributed   Query       **/
    /*********************************/


    /** Parsing partitioning tree from JSON **/
      //TODO read partTreeJsonString from file

    val json: JsValue = Json.parse(partTreeJsonString) //TODO parameter
    val partTreeDeser = deserJsValue("",json)

    println("partTreeDeser =" + partTreeDeser.toJSON)

    val queryFilePath = "/Users/leva/GoogleDrive/INRIA/ADT_IMITATES/workspace/sparkDPiSAX/datasets/seismic_query_10.txt" //TODO parameter
    val queryRDD = sc.textFile(queryFilePath)
                    .map( _.split(',') ).map( t => (t(0).toInt, t.tail.map(_.toFloat) ) )

    val querySAX = tsToSAX(queryRDD)
    val querySAXpart = querySAX.map{case (saxWord, tsId) => (getPartID(saxWord, partRDD.value),(saxWord, tsId))}
//querySAXpart.collect.map(v => (v._1,(v._2._2, v._2._1.mkString("{", ",", "}")))).foreach(println(_))

    val results =  querySAXpart.partitionBy(new HashPartitioner(numPart)).mapPartitions { part =>
      // parse corresponding index tree from JSON
   //   println("query part")
      if (part.hasNext) {
        val first = part.next()
        val jsString = Source.fromFile(partTable(first._1)._1 + ".json").mkString //TODO path to working dir
       // println(jsString)
        val json: JsValue = Json.parse(jsString)
        val root = deserJsValue("", json)

        // get result with centralized approximate search
        var result = Iterator((first._2._2, first._2._1, root.approximateSearch(first._2._1))) // to query first
        result ++= part.map { case (partID, (saxWord, queryId)) => (queryId, saxWord, root.approximateSearch(saxWord)) }  //TODO check ++= on Iterator
       // result.foreach(println)
       //  result.map { case (qid, qw, tslist) => (qid, qw.mkString("<", ",", ">"), tslist.map { case (tsw, tsid) => (tsw.mkString("<", ",", ">"), tsid) }.mkString) }.foreach(println(_))
        result
      }
      else Iterator()
    }
    //  results.collect
    // results.map { case (qid, qw, tslist) => (qid, qw.mkString("<", ",", ">"), tslist.map { case (tsw, tsid) => (tsw.mkString("<", ",", ">"), tsid) }.mkString) }.foreach(println(_))
    //  results.collect
     results.map { case (qid, qw, tslist) => (qid,  tslist.map { case (tsw, tsid) =>  tsid }.mkString(","), tslist.length) }.foreach(println(_))

    //TODO on result join with tsFilePath and calculate actual distance to choose top smth


    //TODO call exact search

  }

  def deserJsValue(jskey: String, jsval: JsValue) : SaxNode = {
    val js = jsval.asInstanceOf[JsObject]
  //  println("_CARD_ = " + js.value("_CARD_").as[String])
    val nodeCard = js.value("_CARD_").as[String].split(",").map(_.toInt)

    if (js.keys.contains("_FILE_")) {
      /** create and return terminal node + read ts_list from file **/
      //println("TN wordToCard = " + jskey.split("_").map(_.split(".").head.toInt).mkString(","))
      //TODO for Querying we don't need the list Of tsIDs, but just link for the file

      val filename = config.workDir + js.value("_FILE_").as[String]
      //TODO  ==> to keep just tsIDs for further querying
      val tsFromFile = Source.fromFile(filename).getLines()
      val tsIDs = tsFromFile.map(_.split(" ")).map(ts => (ts(0).split(",").map(_.toInt).toArray,ts(1).toInt)).toArray
      val wordToCard = jskey.split("_").map(_.split("\\.")(0).toInt)
      new TerminalNode(tsIDs, nodeCard, config.basicSplitBalance(nodeCard), wordToCard)

      //TerminalNode (var tsIDs: Array[(Array[Int],Int)], nodeCard: Array[Int], var splitBalance: Array[Int], wordToCard: Array[Int])
    }
    else {
      var childHash  = new mutable.HashMap[String,SaxNode]()
      js.fields.filterNot(_._1 == "_CARD_").foreach( p => childHash += (p._1 -> deserJsValue(p._1, p._2) ) )
      new InternalNode(nodeCard, childHash)
    }
  }
}
