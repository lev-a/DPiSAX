package fr.inria.zenith

import java.io.PrintWriter

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import play.api.libs.json._

import scala.collection.mutable
import scala.io.Source

/**
  * Created by leva on 24/04/2018.
  */



object DPiSAX  {

  val config = AppConfig(ConfigFactory.load())
  def zeroArray =  Array.fill[Int](config.wordLength)(0)


  private def tsToSAX(ts: RDD[(Int, Array[Float])]): RDD[(Array[Int],Int)] = ts.map(t => (config.tsToSAX(t._2), t._1))

  def partTreeSplit (tree: SaxNode) : Unit = {
    val partTable = tree.partTable.toList
    val partNode = partTable.maxBy(_._3)._1
    tree.partTreeSplit(partNode)
  }

  def runPLS(inputRDD: RDD[(Int, Array[Float])], queryRDD: RDD[(Int, Array[Float])]) : Unit = {

    val plsRDD = (queryRDD cartesian inputRDD).map{ case ((q_id, q_data), (t_id, t_data)) => (q_id, (t_id, config.distance(q_data, t_data))) }
      .combineByKey(v => Array(v)
        , (xs: Array[(Int, Float)], v) => xs :+ v
        , (xs: Array[(Int, Float)], ys: Array[(Int, Float)]) => xs ++ ys)
      .map{ case(q_id, res) => (q_id, res.sortWith(_._2 < _._2).take(config.topk)) }

    val t1 = System.currentTimeMillis()

    println("PLS:")
    plsRDD.map{ case (q_id, res) => "(" + q_id + " " + res.map(r => "(" + r._1 + ", " + r._2 + ")").mkString("<", ",", ">") }.collect().foreach(println(_))

    val t2 = System.currentTimeMillis()
    println("Elapsed time: " + (t2 - t1)  + " ms")

  }


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("DPiSAX")
    val sc: SparkContext = new SparkContext(conf)

    //val tsFilePath = "/Users/leva/Downloads/ts_1000"
    val tsFilePath = "./datasets/Seismic_data.data.csv" //TODO parameter
    val financeData = "./datasets/Finance_Data.csv"

    /*********************************/
    /** Indexing -  Distributed     **/
    /*********************************/
    val inputRDD = sc.textFile(tsFilePath)
                     .map( _.split(',') ).map( t => (t(0).toInt, config.normalize(t.tail.map(_.toFloat)) ) )

    val inputFinanceRDD = sc.textFile(financeData)
      .map( _.split(',') ).map( t => (t(0).toInt, config.normalize(t.tail.tail.map(_.toFloat)) ) )

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

    val queryFilePath = "./datasets/seismic_query_10.txt" //TODO parameter
    val queryRDD = sc.textFile(queryFilePath)
                    .map( _.split(',') ).map( t => (t(0).toInt, config.normalize(t.tail.map(_.toFloat)) ) )


    runPLS(inputRDD, queryRDD)


    val querySAX = queryRDD.map( t => ( t._1, (t._2, config.tsToPAAandSAX(t._2)) ) )
    querySAX.cache()

    val tsLength = querySAX.first()._2._1.length

    val querySAXpart = querySAX.map{case (tsId, (data, (paa, saxWord))) => (getPartID(saxWord, partRDD.value),(saxWord, tsId))}
//querySAXpart.collect.map(v => (v._1,(v._2._2, v._2._1.mkString("{", ",", "}")))).foreach(println(_))

    val results =  querySAXpart.partitionBy(new HashPartitioner(numPart)).mapPartitions { part =>
      // parse corresponding index tree from JSON
   //   println("query part")
      if (part.hasNext) {
        val first = part.next()
        val jsString = Source.fromFile(partRDD.value(first._1)._1._1 + ".json").mkString //TODO path to working dir
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
//     results.map { case (qid, qw, tslist) => (qid,  tslist.map { case (tsw, tsid) =>  tsid }.mkString(","), tslist.length) }.foreach(println(_))

    val approxRDD = results.flatMap{ case (qid, qw, tslist) => tslist.map( t => (t._2, qid) ) }
      .join(inputRDD)
      .map{ case(tsid, (qid, tsdata)) => (qid, (tsid, tsdata)) }
      .combineByKey(v => Array(v)
        , (xs:Array[(Int, Array[Float])], v) => xs :+ v
        , (xs:Array[(Int, Array[Float])], ys:Array[(Int, Array[Float])]) => xs ++ ys)
      .join(querySAX)
      .map{ case(qid, (tslist, (qdata, (q_paa, q_sax)))) => ( qid, q_paa, qdata, tslist.map( t => (t._1, t._2, config.distance(qdata, t._2)) ).sortBy(_._3).take(config.topk) ) }

    approxRDD.cache()

    val t3 = System.currentTimeMillis()

    println("\nApproximate:")
    approxRDD.collect().map{ case (q_id, q_paa, qdata, res) => "(" + q_id + " " + res.map(r => "(" + r._1 + ", " + r._3 + ")").mkString("<", ",", ">") }.foreach(println(_))

    val t4 = System.currentTimeMillis()
    println("Elapsed time: " + (t4 - t3)  + " ms")


    /*********************************/
    /**  Exact Search       **/
    /*********************************/

    val queryExact = sc.parallelize(0 until numPart) cartesian approxRDD.filter(_._3.length > 0).map{ case (qid, q_paa, qdata, res) => (qid, q_paa, res.last._3) } // TODO: switch to full search whwn approx gives no result

    val resultsExact = queryExact.partitionBy(new HashPartitioner(numPart)).mapPartitions { part =>
      // parse corresponding index tree from JSON
      //   println("query part")
      if (part.hasNext) {
        val first = part.next()
        val jsString = Source.fromFile(partRDD.value(first._1)._1._1 + ".json").mkString //TODO path to working dir
        // println(jsString)
        val json: JsValue = Json.parse(jsString)
        val root = deserJsValue("", json)

        // get result with centralized approximate search
        var result = Iterator((first._2._1, root.boundedSearch(first._2._2, first._2._3, tsLength))) // to query first
        result ++= part.map { case (partId, (queryId, queryPAA, queryBound)) => (queryId, root.boundedSearch(queryPAA, queryBound, tsLength)) }  //TODO check ++= on Iterator
        // result.foreach(println)
        //  result.map { case (qid, qw, tslist) => (qid, qw.mkString("<", ",", ">"), tslist.map { case (tsw, tsid) => (tsw.mkString("<", ",", ">"), tsid) }.mkString) }.foreach(println(_))
        result
      }
      else Iterator()
    }

/*
    val exactRDD = resultsExact.flatMap{ case (qid, tslist) => tslist.map( t => (t._2, qid) ) }
      .join(inputRDD)
      .map{ case(tsid, (qid, tsdata)) => (qid, (tsid, tsdata)) }
      .combineByKey(v => Array(v), (xs:Array[(Int, Array[Float])], v) => xs :+ v, (xs:Array[(Int, Array[Float])], ys:Array[(Int, Array[Float])]) => xs ++ ys)
      .join(querySAX)
      .map{ case(qid, (tslist, (qdata, (q_paa, q_sax)))) => ( qid, tslist.map( t => (t._1, t._2, config.distance(qdata, t._2)) ).sortBy(_._3).take(topk) ) }
*/

    val exactRDD = resultsExact.map(r => (r._1, r._2.map(_._2))) // keep only ids
      .join(querySAX).flatMap{ case(q_id, (res, (q_data, (q_paa, q_sax)))) => res.map(t_id => (t_id, (q_id, q_paa, q_data))) }
      .join(inputRDD).map{ case(t_id, ((q_id, q_paa, q_data), t_data)) => (q_id, (t_id, config.distance(q_data, t_data))) }
      .combineByKey(v => Array(v)
        , (xs: Array[(Int, Float)], v) => xs :+ v
        , (xs: Array[(Int, Float)], ys: Array[(Int, Float)]) => xs ++ ys)
      .map{ case(q_id, res) => (q_id, res.sortWith(_._2 < _._2).take(config.topk)) }

    val t5 = System.currentTimeMillis()

    println("\nExact:")
    exactRDD.map{ case (q_id, res) => "(" + q_id + " " + res.map(r => "(" + r._1 + ", " + r._2 + ")").mkString("<", ",", ">") }.collect().foreach(println(_))

    val t6 = System.currentTimeMillis()
    println("Elapsed time: " + (t6 - t5)  + " ms")

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
