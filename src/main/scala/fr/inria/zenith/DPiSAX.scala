package fr.inria.zenith

import java.io.PrintWriter

import com.typesafe.config.ConfigFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import play.api.libs.json._

import scala.collection.mutable
import scala.io.Source

/**
  * Created by leva on 24/04/2018.
  */



object DPiSAX  {

  type CombineData = (Array[Float], Array[(Int, Array[Float], Float)])
  type CombineDataPAA = (Array[Float], Array[Float], Array[(Int, Array[Float], Float)])

  type DataRDD = RDD[(Int, Array[Float])]
  type DataStatsRDD = RDD[(Int, (Array[Float], Float, Float))]
  type ApproxRDD = RDD[(Int, Array[Float], Array[Float], Array[(Int, Array[Float], Float)])]
  type OutputRDD = RDD[(Int, Array[Float], Array[(Int, Array[Float], Float)])]

  type PartTableBC = Broadcast[Array[((String, Array[Int]), Int)]]


  val config = AppConfig(ConfigFactory.load())
//  def zeroArray =  Array.fill[Int](config.wordLength)(0)


  private def readRDD(sc: SparkContext, tsFile: String) = {
   // val tsFilePath = config.tsFilePath
    val numPart = config.numPart
    val firstCol = config.firstCol

    val distFile = if ( Array("txt", "csv").contains(tsFile.slice(tsFile.length - 3, tsFile.length)) )
      (if (numPart == 0) sc.textFile(tsFile) else sc.textFile(tsFile, numPart))
        .map( _.split(',') ).map( t => (t(0).toInt, t.slice(firstCol, t.length).map(_.toFloat) ) )
    else
      if (numPart == 0) sc.objectFile[(Int, (Array[Float]))](tsFile)
      else sc.objectFile[(Int, (Array[Float]))](tsFile, numPart)

    distFile.map(ts => (ts._1, config.normalize(ts._2)))
    // TODO: work on data with stats instead of normalized data
  }

  private def outputRDD(label: String, rdd: OutputRDD) : Unit = {

    val t1 = System.currentTimeMillis()

    println(label + ":")
    rdd.map{ case (q_id, q_data, res) => "(" + q_id + " " + res.map(r => "(" + r._1 + ", " + r._3 + ")").mkString("<", ",", ">") }
      .collect()
      .foreach(println(_))

    val t2 = System.currentTimeMillis()
    println("Elapsed time: " + (t2 - t1)  + " ms")

  }


  private def tsToSAX(ts: RDD[(Int, Array[Float])]): RDD[(Array[Int],Int)] = ts.map(t => (config.tsToSAX(t._2), t._1))

  private def partTreeSplit (tree: SaxNode) : Unit = {
    val partTable = tree.partTable.toList
    val partNode = partTable.maxBy(_._3)._1
    tree.partTreeSplit(partNode)
  }

  private def linearSearch(inputRDD: DataRDD, queryRDD: DataRDD) = {

    val plsRDD = (queryRDD cartesian inputRDD).map{ case ((q_id, q_data), (t_id, t_data)) => (q_id, (q_data, t_id, t_data, config.distance(q_data, t_data))) }
      .combineByKey(v => (v._1, Array((v._2, v._3, v._4)))
        , (xs: CombineData, v) => (xs._1, xs._2 :+ (v._2, v._3, v._4))
        , (xs: CombineData, ys: CombineData) => (xs._1, xs._2 ++ ys._2))
      .map{ case(q_id, (q_data, res)) => (q_id, q_data, res.sortBy(_._3).take(config.topk)) }

    plsRDD
  }

  def deserJsValue(jskey: String, jsval: JsValue) : SaxNode = {
    val js = jsval.asInstanceOf[JsObject]
    //  println("_CARD_ = " + js.value("_CARD_").as[String])
    val nodeCard = js.value("_CARD_").as[String].split(",").map(_.toInt)

    if (js.keys.contains("_FILE_")) {
      /** create and return terminal node + read ts_list from file **/
      //println("TN wordToCard = " + jskey.split("_").map(_.split(".").head.toInt).mkString(","))

      val filename = config.workDir + js.value("_FILE_").as[String]
      val tsFromFile = Source.fromFile(filename).getLines()
      val tsIDs = tsFromFile.map(_.split(" ")).map(ts => (ts(0).split(",").map(_.toInt).toArray,ts(1).toInt)).toArray
      val wordToCard = jskey.split("_").map(_.split("\\.")(0).toInt)
      new TerminalNode(tsIDs, nodeCard, config.basicSplitBalance(nodeCard), wordToCard)
    }
    else {
      var childHash  = new mutable.HashMap[String,SaxNode]()
      js.fields.filterNot(_._1 == "_CARD_").foreach( p => childHash += (p._1 -> deserJsValue(p._1, p._2) ) )
      new InternalNode(nodeCard, childHash)
    }
  }

  private def createPartTable(inputRDD: DataRDD) = {

    /** Sampling **/
    val tsSample = inputRDD.sample(false,config.sampleSize)
    val sampleToSAX = tsToSAX(tsSample)

    var partTree = new TerminalNode(Array.empty, config.zeroArray, config.basicSplitBalance(config.zeroArray), config.zeroArray )
    sampleToSAX.collect.foreach{case (saxWord, tsId) => partTree.insert(saxWord, tsId)}
    //  println (partTree.toJSON)
    val partTreeRoot  = partTree.split()
    //    println (partTreeRoot.toJSON)

    0 until config.numPart-2 foreach { _ =>  partTreeSplit(partTreeRoot) }

    /** Partitioning TAble **/
    val partTable = partTreeRoot.partTable
    val partTreeJsonString = partTreeRoot.toJSON //TODO save to file
    println ("partTreeJsonString = " + partTreeJsonString)
    //partTable.foreach(println)
    // partTable.map(v => (v._1, v._2.mkString("{",",","}"), v._3)).foreach(println)

    /** Parsing partitioning tree from JSON **/
    val json: JsValue = Json.parse(partTreeJsonString) //TODO parameter
    val partTreeDeser = deserJsValue("",json)

    // println("partTreeDeser =" + partTreeDeser.toJSON)

    partTable
  }

  private def getPartID(tsWord: Array[Int], partTable: Array[((String, Array[Int]), Int)]): Int = {
    def nodeID (saxWord: Array[Int], nodeCard: Array[Int]) = (saxWord zip nodeCard).map {  case (w, c) => (w >> (config.maxCardSymb - c)) + "." + c}.mkString("_")
    partTable.find(v => v._1._1 == nodeID(tsWord, v._1._2)).map(_._2).getOrElse(-1)
  }

  private def buildIndex(partRDD: PartTableBC, inputRDD: DataRDD) : Unit = {
    val inputSAX = tsToSAX(inputRDD)
    val inputSAXpart = inputSAX.map{case (saxWord, tsId) => (getPartID(saxWord, partRDD.value), (saxWord, tsId))}

    //  inputSAXpart.take(20).foreach(println)

    //val roots =
    inputSAXpart.partitionBy(new HashPartitioner(config.numPart)).foreachPartition {part =>
      val first =  part.next()
      val root = new InternalNode(partRDD.value(first._1)._1._2.map(_+1),mutable.HashMap.empty)
      root.insert(first._2._1, first._2._2)
      part.foreach{case (partID, (saxWord, tsId))  => root.insert(saxWord, tsId)}
      // save to file ' partTable(first._1)._1 + ".json"'
      new PrintWriter(config.workDir + partRDD.value(first._1)._1._1 + ".json") { write(root.toJSON); close }
      Iterator(root.toJSON)
    }
    // roots.foreach(println)

  }

  private def approximateQuery(partRDD: PartTableBC, inputRDD: DataRDD, queryRDD: DataRDD) : ApproxRDD = {

    val querySAX = queryRDD.map( t => ( t._1, (t._2, config.tsToPAAandSAX(t._2)) ) )

    val querySAXpart = querySAX.map{case (tsId, (data, (paa, saxWord))) => (getPartID(saxWord, partRDD.value), (saxWord, tsId, paa, data))}
    //querySAXpart.collect.map(v => (v._1,(v._2._2, v._2._1.mkString("{", ",", "}")))).foreach(println(_))

    val results =  querySAXpart.partitionBy(new HashPartitioner(config.numPart)).mapPartitions { part =>
      // parse corresponding index tree from JSON
      //   println("query part")
      if (part.hasNext) {
        val first = part.next()
        val jsString = Source.fromFile(config.workDir + partRDD.value(first._1)._1._1 + ".json").mkString
        // println(jsString)
        val json: JsValue = Json.parse(jsString)
        val root = deserJsValue("", json)

        // get result with centralized approximate search
        var result = Iterator((first._2._2, first._2._1, root.approximateSearch(first._2._1), first._2._3, first._2._4)) // to query first
        result ++= part.map { case (partID, (saxWord, queryId, paa, data)) => (queryId, saxWord, root.approximateSearch(saxWord), paa, data) }
        // result.foreach(println)
        //  result.map { case (qid, qw, tslist) => (qid, qw.mkString("<", ",", ">"), tslist.map { case (tsw, tsid) => (tsw.mkString("<", ",", ">"), tsid) }.mkString) }.foreach(println(_))
        result
      }
      else Iterator()
    }
    //  results.collect
    // results.map { case (qid, qw, tslist) => (qid, qw.mkString("<", ",", ">"), tslist.map { case (tsw, tsid) => (tsw.mkString("<", ",", ">"), tsid) }.mkString) }.foreach(println(_))
    //  results.collect
    //     results.map { case (qid, qw, tslist) => (qid,  tslist.map { case (tsw, tsid) =>  tsid }.mkString(","), tslist.length) }.foreach(println(_))

    val approxRDD = results.flatMap{ case (qid, qw, tslist, paa, data) => tslist.map( t => (t._2, (qid, paa, data)) ) }
      .join(inputRDD)
      .map{case(tsid, ((qid, qpaa, qdata), tsdata)) => (qid, (qpaa, qdata, tsid, tsdata, config.distance(qdata, tsdata)))}
      .combineByKey(v => (v._1, v._2, Array((v._3, v._4, v._5)))
        , (xs: CombineDataPAA, v) => (xs._1, xs._2, xs._3 :+ (v._3, v._4, v._5))
        , (xs: CombineDataPAA, ys: CombineDataPAA) => (xs._1, xs._2, xs._3 ++ ys._3))
      .map{ case(q_id, (q_paa, q_data, res)) => (q_id, q_paa, q_data, res.sortBy(_._3).take(config.topk)) }

    approxRDD
  }

  private def exactQuery(partRDD: PartTableBC, inputRDD: DataRDD, approxRDD: ApproxRDD, sc: SparkContext) : OutputRDD = {

    val queryExact = sc.parallelize(0 until config.numPart) cartesian approxRDD.filter(_._3.length > 0).map{ case (qid, q_paa, qdata, res) => (qid, q_paa, res.last._3, qdata) }
    // TODO: switch to full search when approx gives no result
    // TODO: run boundedSearch on batches of queries

    val resultsExact = queryExact.partitionBy(new HashPartitioner(config.numPart)).mapPartitions { part =>
      if (part.hasNext) {
        val first = part.next()
        val jsString = Source.fromFile(config.workDir + partRDD.value(first._1)._1._1 + ".json").mkString
        val json: JsValue = Json.parse(jsString)
        val root = deserJsValue("", json)

        val tsLength = first._2._4.length

        // get result with centralized approximate search
        var result = Iterator((first._2._1, root.boundedSearch(first._2._2, first._2._3, tsLength), first._2._4)) // to query first
        result ++= part.map { case (partId, (queryId, queryPAA, queryBound, queryData)) => (queryId, root.boundedSearch(queryPAA, queryBound, tsLength), queryData) }
        result
      }
      else Iterator()
    }

    val exactRDD = resultsExact.flatMap{ case(qid, tslist, qdata) => tslist.map( t => (t._2, (qid, qdata)) )}
      .join(inputRDD)
      .map{ case(t_id, ((q_id, q_data), t_data)) => (q_id, (q_data, t_id, t_data, config.distance(q_data, t_data))) }
      .combineByKey(v => (v._1, Array((v._2, v._3, v._4)))
        , (xs: CombineData, v) => (xs._1, xs._2 :+ (v._2, v._3, v._4))
        , (xs: CombineData, ys: CombineData) => (xs._1, xs._2 ++ ys._2))
      .map{ case(q_id, (q_data, res)) => (q_id, q_data, res.sortBy(_._3).take(config.topk)) }

    exactRDD
  }

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("DPiSAX")  //TODO move to AppConfig
    val sc: SparkContext = new SparkContext(conf)

    //val tsFilePath = "/Users/leva/Downloads/ts_1000"
  //  val tsFilePath = "./datasets/Seismic_data.data.csv" //TODO parameter
  // val financeData = "./datasets/Finance_Data.csv"

    val tsFilePath = config.tsFilePath
    val inputRDD = readRDD(sc, tsFilePath)

    val queryFilePath = config.queryFilePath
    val queryRDD = readRDD(sc, queryFilePath)


    /*********************************/
    /** Parallel Linear Search      **/
    /*********************************/

    val plsRDD = linearSearch(inputRDD, queryRDD)
    outputRDD("PLS", plsRDD)


    /*********************************/
    /** Indexing -  Distributed     **/
    /*********************************/

    /** Building partitioning tree  **/
  //  val numPart = config.numPart

    val partTable = createPartTable(inputRDD)
    val partRDD : PartTableBC = sc.broadcast(partTable.map(col => (col._1, col._2)).zipWithIndex)

    /** Partitioning of input dataset **/

    buildIndex(partRDD, inputRDD)


    /*********************************/
    /**  Distributed   Query       **/
    /*********************************/

    // TODO read partTreeJsonString from file (to create partRDD)

    val approxRDD = approximateQuery(partRDD, inputRDD, queryRDD)

    approxRDD.cache()

    outputRDD( "Approximate", approxRDD.map{ case (q_id, q_paa, qdata, res) => (q_id, qdata, res) } )


    /*********************************/
    /**  Exact Search               **/
    /*********************************/

    val exactRDD = exactQuery(partRDD, inputRDD, approxRDD, sc)
    outputRDD("Exact", exactRDD)

  }
}
