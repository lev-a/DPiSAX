package fr.inria.zenith

import java.io.{BufferedReader, InputStreamReader, PrintWriter}

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import play.api.libs.json._

import scala.collection.mutable



object DPiSAX  {

  type CombineData = (Array[Float], Array[(Long, Array[Float], Float)])
  type CombineDataPAA = (Array[Float], (Array[Float], (Float, Float)), Array[(Long, Array[Float], Float)])

  type DataStatsRDD = RDD[(Long, (Array[Float], (Float, Float)))]
  type ApproxRDD = RDD[(Long, Array[Float], (Array[Float], (Float, Float)), Array[(Long, Array[Float], Float)])]
  type OutputRDD = RDD[(Long, Array[Float], Array[(Long, Array[Float], Float)])]

  type PartTableBC = Broadcast[Array[((String, Array[Int]), Int)]]


  val config = AppConfig(ConfigFactory.load())
  var fsURI: String = null
  var numPart: Int = 0

  private def readRDD(sc: SparkContext, tsFile: String) : DataStatsRDD = {

    val firstCol = config.firstCol

    val distFile = if ( Array("txt", "csv").contains(tsFile.slice(tsFile.length - 3, tsFile.length)) )
      (if (numPart == 0) sc.textFile(tsFile) else sc.textFile(tsFile, numPart))
        .map( _.split(',') ).map( t => (t(0).toLong, t.slice(firstCol, t.length).map(_.toFloat) ) )
    else
      if (numPart == 0) sc.objectFile[(Long, (Array[Float]))](tsFile)
      else sc.objectFile[(Long, (Array[Float]))](tsFile, numPart)

    distFile.map( ts => (ts._1, (ts._2, config.stats(ts._2))) )
  }

  private def outputRDD(label: String, rdd: OutputRDD) : Unit = {
    //TODO => save full result of query to file
    val t1 = System.currentTimeMillis()

    println(label + ":")
    rdd.map{ case (q_id, q_data, res) => "(" + q_id + " " + res.map(r => "(" + r._1 + ", " + r._3 + ")").mkString("<", ",", ">") }
      .collect()
      .foreach(println(_))

    val t2 = System.currentTimeMillis()
    println("Elapsed time: " + (t2 - t1)  + " ms")

  }


  private def tsToSAX(ts: DataStatsRDD): RDD[(Array[Int], Long)] = ts.map(t => (config.tsToSAX(t._2), t._1))

  private def partTreeSplit (tree: SaxNode) : Unit = {
    val partTable = tree.partTable.toList
    val partNode = partTable.maxBy(_._3)._1
    tree.partTreeSplit(partNode)
  }

  private def linearSearch(inputRDD: DataStatsRDD, queryRDD: DataStatsRDD) : OutputRDD = {

    val plsRDD = (queryRDD cartesian inputRDD).map{ case ((q_id, q_data), (t_id, t_data)) => (q_id, (q_data._1, t_id, t_data._1, config.distance(q_data, t_data))) }
      .combineByKey(v => (v._1, Array((v._2, v._3, v._4)))
        , (xs: CombineData, v) => (xs._1, xs._2 :+ (v._2, v._3, v._4))
        , (xs: CombineData, ys: CombineData) => (xs._1, xs._2 ++ ys._2))
      .map{ case(q_id, (q_data, res)) => (q_id, q_data, res.sortBy(_._3).take(config.topk)) }

    plsRDD
  }

  def deserJsValue(jskey: String, jsval: JsValue, fsURI: String) : SaxNode = {
    val js = jsval.asInstanceOf[JsObject]
    val nodeCard = js.value("_CARD_").as[String].split(",").map(_.toInt)

    if (js.keys.contains("_FILE_")) {
      /** create and return terminal node + read ts_list from file **/
      val filename = config.workDir + js.value("_FILE_").as[String]
      val tsFromFile = setReader(fsURI, filename)
      val tsIDs = tsFromFile.map(_.split(" ")).map(ts => (ts(0).split(",").map(_.toInt).toArray,ts(1).toLong)).toArray
      val wordToCard = jskey.split("_").map(_.split("\\.")(0).toInt)
      new TerminalNode(tsIDs, nodeCard, config.basicSplitBalance(nodeCard), wordToCard)
    }
    else {
      var childHash  = new mutable.HashMap[String,SaxNode]()
      js.fields.filterNot(_._1 == "_CARD_").foreach( p => childHash += (p._1 -> deserJsValue(p._1, p._2, fsURI) ) )
      new InternalNode(nodeCard, childHash)
    }
  }

  private def createPartTable(inputRDD: DataStatsRDD) = {

    val fscopy = fsURI

    /** Sampling **/
    val tsSample = inputRDD.sample(false,config.sampleSize)
    val sampleToSAX = tsToSAX(tsSample)

    println("numPart =  " + numPart)

    val partTree = new TerminalNode(Array.empty, config.zeroArray, config.basicSplitBalance(config.zeroArray), config.zeroArray )
    sampleToSAX.collect.foreach{case (saxWord, tsId) => partTree.insert(saxWord, tsId)}
    val partTreeRoot  = partTree.split()

    0 until numPart-2 foreach { _ =>  partTreeSplit(partTreeRoot) }

    /** Partitioning TAble **/
    val partTable = partTreeRoot.partTable
    /** saves partTable to file 'workDir/partTable **/
    var writer = setWriter(fscopy, config.workDir + "partTable")
      partTable.foreach{case (nodeID, nodeCard, tsNum) => writer.write (nodeID + " " + nodeCard.mkString(",") + " " + tsNum + "\n") }
      writer.close

    /** saves partTree to file 'workDir/partTree' **/
    writer = setWriter(fscopy, config.workDir + "partTree.json")
      writer.write(partTreeRoot.toJSON(fscopy)); writer.close
    println("partTable:" ) ;partTable.map(v => (v._1, v._2.mkString("{",",","}"), v._3)).foreach(println)

    partTable
  }

  private def readPartTable()  = {
    /** reads partTable ( Array[ (String,Array[Int],Int)]) from file  'workDir/partTable' **/
    val fscopy = fsURI
    val partFromFile = setReader(fscopy, config.workDir + "partTable")
    val partTable = partFromFile.map(_.split(" ")).map{part => (part(0), part(1).split(",").map(_.toInt).toArray, part(2).toInt)}.toArray
    println("partTable from file:" ); partTable.map(v => (v._1, v._2.mkString("{",",","}"), v._3)).foreach(println)

    /** [Optional] Parsing partitioning tree from JSON **/
      /*
    val partTreeJsonString = setReader(fscopy,config.workDir + "partTree.json").mkString
    val json: JsValue = Json.parse(partTreeJsonString)
    val partTreeDeser = deserJsValue("",json,fscopy)
    */
    // println("partTreeDeser =" + partTreeDeser.toJSON)

    partTable
  }

  private def getPartID(tsWord: Array[Int], partTable: Array[((String, Array[Int]), Int)]): Int = {
    def nodeID (saxWord: Array[Int], nodeCard: Array[Int]) = (saxWord zip nodeCard).map {  case (w, c) => (w >> (config.maxCardSymb - c)) + "." + c}.mkString("_")
    partTable.find(v => v._1._1 == nodeID(tsWord, v._1._2)).map(_._2).getOrElse(-1)
  }

  private def buildIndex(partRDD: PartTableBC, inputRDD: DataStatsRDD) : Unit = {
    val inputSAX = tsToSAX(inputRDD)
    val inputSAXpart = inputSAX.map{case (saxWord, tsId) => (getPartID(saxWord, partRDD.value), (saxWord, tsId))}
    val fscopy = fsURI

    inputSAXpart.partitionBy(new HashPartitioner(numPart)).foreachPartition {part =>
      val first =  part.next()
      val root = new InternalNode(partRDD.value(first._1)._1._2.map(_+1),mutable.HashMap.empty)
      root.insert(first._2._1, first._2._2)
      part.foreach{case (partID, (saxWord, tsId))  => root.insert(saxWord, tsId)}
      val writer = setWriter(fscopy, config.workDir + partRDD.value(first._1)._1._1 + ".json")
      writer.write(root.toJSON(fscopy)); writer.close
    }
  }

  private def approximateQuery(partRDD: PartTableBC, inputRDD: DataStatsRDD, queryRDD: DataStatsRDD) : ApproxRDD = {

    val querySAX = queryRDD.map( t => ( t._1, (t._2, config.tsToPAAandSAX(t._2)) ) )
    val querySAXpart = querySAX.map{case (tsId, (data, (paa, saxWord))) => (getPartID(saxWord, partRDD.value), (saxWord, tsId, paa, data))}
    val fscopy = fsURI

    val results =  querySAXpart.partitionBy(new HashPartitioner(numPart)).mapPartitions { part =>
      if (part.hasNext) {
        val first = part.next()
        val jsString = setReader(fscopy, config.workDir + partRDD.value(first._1)._1._1 + ".json").mkString

        val json: JsValue = Json.parse(jsString)
        val root = deserJsValue("", json, fscopy)

        var result = Iterator((first._2._2, first._2._1, root.approximateSearch(first._2._1), first._2._3, first._2._4))
        result ++= part.map { case (partID, (saxWord, queryId, paa, data)) => (queryId, saxWord, root.approximateSearch(saxWord), paa, data) }
        result
      }
      else Iterator()
    }

    val approxRDD = results.flatMap{ case (qid, qw, tslist, paa, data) => tslist.map( t => (t._2, (qid, paa, data)) ) }
      .join(inputRDD)
      .map{case(tsid, ((q_id, q_paa, q_data), t_data)) => (q_id, (q_paa, q_data, tsid, t_data._1, config.distance(q_data, t_data)))}
      .combineByKey(v => (v._1, v._2, Array((v._3, v._4, v._5)))
        , (xs: CombineDataPAA, v) => (xs._1, xs._2, xs._3 :+ (v._3, v._4, v._5))
        , (xs: CombineDataPAA, ys: CombineDataPAA) => (xs._1, xs._2, xs._3 ++ ys._3))
      .map{ case(q_id, (q_paa, q_data, res)) => (q_id, q_paa, q_data, res.sortBy(_._3).take(config.topk)) }

    approxRDD
  }

  private def exactQuery(partRDD: PartTableBC, inputRDD: DataStatsRDD, approxRDD: ApproxRDD, partIndexRDD: RDD[Int]) : OutputRDD = {

    val queryExact = partIndexRDD cartesian approxRDD.filter(_._4.length > 0).map{ case (q_id, q_paa, q_data, res) => (q_id, q_paa, res.last._3, q_data) }
    // TODO: switch to full search when approx gives no result
    // TODO: run boundedSearch on batches of queries

    val fscopy = fsURI
    val resultsExact = queryExact.partitionBy(new HashPartitioner(numPart)).mapPartitions { part =>
      if (part.hasNext) {
        val first = part.next()
        val jsString = setReader(fscopy, config.workDir + partRDD.value(first._1)._1._1 + ".json").mkString
        val json: JsValue = Json.parse(jsString)
        val root = deserJsValue("", json, fscopy)

        val tsLength = first._2._4._1.length

        var result = Iterator((first._2._1, root.boundedSearch(first._2._2, first._2._3, tsLength), first._2._4))
        result ++= part.map { case (partId, (queryId, queryPAA, queryBound, queryData)) => (queryId, root.boundedSearch(queryPAA, queryBound, tsLength), queryData) }
        result
      }
      else Iterator()
    }

    // TODO: compare performance with (resultsExact join querySAX join inputRDD)

    val exactRDD = resultsExact.flatMap{ case(qid, tslist, qdata) => tslist.map( t => (t._2, (qid, qdata)) )}
      .join(inputRDD)
      .map{ case(t_id, ((q_id, q_data), t_data)) => (q_id, (q_data._1, t_id, t_data._1, config.distance(q_data, t_data))) }
      .combineByKey(v => (v._1, Array((v._2, v._3, v._4)))
        , (xs: CombineData, v) => (xs._1, xs._2 :+ (v._2, v._3, v._4))
        , (xs: CombineData, ys: CombineData) => (xs._1, xs._2 ++ ys._2))
      .map{ case(q_id, (q_data, res)) => (q_id, q_data, res.sortBy(_._3).take(config.topk)) }

    exactRDD
  }
  def getFS (fsURI: String) = {   //TODO => move to Object Utils
    val conf = new Configuration()
    conf.set("fs.defaultFS", fsURI)
    FileSystem.get(conf)
  }

  def setWriter(fsURI: String, path: String) : PrintWriter = {   //TODO => move to Object Utils

    val fs = getFS(fsURI)
    val output = fs.create(new Path(path))
    new PrintWriter(output)
  }

  def setReader(fsURI: String, path: String) : Array[String]  = {  //TODO => move to Object Utils
    val fs = getFS(fsURI)
    val input = new Path(path)
    fs.exists(input) match {
      //case true => sc.textFile(path).collect
      case true => {
        val reader = new BufferedReader(new InputStreamReader(fs.open(input)))
        val lines = scala.collection.mutable.ArrayBuffer[String]()
        var line = reader.readLine()
        while (line != null) {
          lines += line
          line = reader.readLine()
        }
        reader.close; //fs.close
        lines.toArray
      }
      case false => /*fs.close;*/  Array[String]()
    }
  }


  def main(args: Array[String]): Unit = {

   val conf: SparkConf = new SparkConf().setAppName("DPiSAX")
   val sc = new SparkContext(conf)
   fsURI  = sc.hadoopConfiguration.get("fs.defaultFS")


    val tsFilePath = config.tsFilePath
    val inputRDD = readRDD(sc, tsFilePath)

    val queryFilePath = config.queryFilePath
    val queryRDD = readRDD(sc, queryFilePath)

    println(tsFilePath)
    println("workDir: " + config.workDir)

    /*********************************/
    /** Parallel Linear Search      **/
    /*********************************/

    val plsRDD = linearSearch(inputRDD, queryRDD)
    outputRDD("PLS", plsRDD)

    numPart = config.numPart==0 match {
      case true  => sc.defaultParallelism//executors * coresPerEx
      case false => config.numPart
    }

    /*********************************/
    /** Indexing -  Distributed     **/
    /*********************************/

    /** Building partitioning tree  **/

  /** checks if partTable already exists for the given input, and skips building table, just reads from file **/
    val fs = getFS(fsURI)
    val partTable =  fs.exists( new Path(config.workDir)) match {
      case true => readPartTable() //TODO => assume that index already build and skip buildIndex ???
      case false => createPartTable(inputRDD)
    }


//   val partTable = createPartTable(inputRDD)
    val partRDD : PartTableBC = sc.broadcast(partTable.map(col => (col._1, col._2)).zipWithIndex) //TODO => partTable separate for  indexing and querying ???

    /** Partitioning of input dataset **/

    buildIndex(partRDD, inputRDD) //TODO => to measure exec time


    /*********************************/
    /**  Distributed   Query       **/
    /*********************************/


    val approxRDD = approximateQuery(partRDD, inputRDD, queryRDD)

    approxRDD.cache()

    outputRDD( "Approximate", approxRDD.map{ case (q_id, q_paa, q_data, res) => (q_id, q_data._1, res) } )


    /*********************************/
    /**  Exact Search               **/
    /*********************************/

    val exactRDD = exactQuery(partRDD, inputRDD, approxRDD, sc.parallelize(0 until numPart))
    outputRDD("Exact", exactRDD)

  }
}
