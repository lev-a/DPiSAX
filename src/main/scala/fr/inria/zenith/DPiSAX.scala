package fr.inria.zenith

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
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

    val t1 = System.currentTimeMillis()

 //   println(label + ":")

 //   rdd.map{ case (q_id, q_data, res) => "(" + q_id + " " + res.map(r => "(" + r._1 + ", " + r._3 + ")").mkString("<", ",", ">") }
 //     .collect()
 //     .foreach(println(_))

   //val res =   rdd.map{ case (q_id, q_data, res) => "(" + q_id + "," + q_data.mkString("[",",","]") + ")," + res.map(c => "(" + c._1 +  "," + c._2.mkString("[",",","]") + ")," + c._3 ).mkString("(",",",")")}
    val res =   rdd.map{ case (q_id, q_data, res) =>((q_id, q_data.mkString("[",",","]")),res.map(c => "(" + c._1 +  "," + c._2.mkString("[",",","]") + ")," + c._3 ).mkString("(",",",")"))}
    .collect

    val t2 = System.currentTimeMillis()
    val writer = Utils.setWriter("file:///", "/tmp/" + config.workDir + Utils.getFileName(config.queryFilePath) + "_" + label + "_" + (t2 - t1) )
    writer.write(res.map(_.toString).reduce(_ + '\n' + _))
    writer.write("\n")
    writer.close

    println(label + ": " + (t2 - t1)  + " ms (" +  Utils.getMinSec(t2-t1) + ")")

  }


  private def mergeDistances(xs: Array[(Long, Array[Float], Float)], ys: Array[(Long, Array[Float], Float)]) = {
    var rs = new mutable.ListBuffer[(Long, Array[Float], Float)]()
    var i = 0

    for (x <- xs) {
      while (i < ys.length && x._3 > ys(i)._3) {
        rs += ys(i)
        i += 1
      }

      rs += x
    }

    if (i < ys.length)
      rs ++= ys.slice(i, ys.length)

    rs.take(config.topk).toArray
  }

  private def tsToSAX(ts: DataStatsRDD): RDD[(Array[Int], Long)] = ts.map(t => (config.tsToSAX(t._2), t._1))

  private def partTreeSplit (tree: SaxNode) : Unit = {
    val partTable = tree.partTable.toList
    val partNode = partTable.maxBy(_._3)._1
    if (partNode != null)
      tree.partTreeSplit(partNode)
  }

  private def linearSearch(inputRDD: DataStatsRDD, queryRDD: DataStatsRDD, sc: SparkContext) : OutputRDD = {

    val queryBC = sc.broadcast( queryRDD.collectAsMap() )

    val plsRDD = inputRDD.mapPartitions{ part =>
        var queryMap = new mutable.HashMap[Long, Array[(Long, Array[Float], Float)]]()
        queryBC.value.foreach(q => queryMap += (q._1 -> Array.empty))

        while (part.hasNext) {
          val (t_id, t_data) = part.next()
          queryBC.value.foreach{ case (q_id, q_data) => queryMap(q_id) = mergeDistances(queryMap(q_id), Array((t_id, t_data._1, config.distance(q_data, t_data)))) }
        }

        queryMap.toIterator
      }
      .reduceByKey(mergeDistances)
      .map{ case(q_id, res) => (q_id, queryBC.value(q_id)._1, res)}

    plsRDD
  }

  def deserJsValue(jskey: String, jsval: JsValue, fsURI: String) : SaxNode = {
    val js = jsval.asInstanceOf[JsObject]
//    val nodeCard = js.value("_CARD_").as[String].split(",").map(_.toInt)
    val (wordToCard, nodeCard) = config.parseNodeId(jskey)

    if (js.keys.contains("_FILE_")) {
      /** create and return terminal node + read ts_list from file **/
      val filename = config.workDir + js.value("_FILE_").as[String]
      val tsFromFile = Utils.setReader(fsURI, filename)
      val tsIDs = tsFromFile.map(_.split(" ")).map(ts => (ts(0).split(",").map(_.toInt).toArray,ts(1).toLong)).toArray
//      val wordToCard = jskey.split("_").map(_.split("\\.")(0).toInt)
      new TerminalNode(tsIDs, nodeCard, wordToCard)
    }
    else {
      var childHash  = new mutable.HashMap[String,SaxNode]()
      js.fields.filterNot(_._1 == "_CARD_").foreach( p => childHash += (p._1 -> deserJsValue(p._1, p._2, fsURI) ) )
      val childCard = js.value("_CARD_").as[String].split(",").map(_.toInt)
      new InternalNode(childCard, childHash, nodeCard, wordToCard)
    }
  }

  private def createPartTable(inputRDD: DataStatsRDD) = {

    val fscopy = fsURI

    //val sample = config.sampleSize
   // val sample = numPart*1000
    /** Sampling **/
    val tsSample = inputRDD.sample(false,config.sampleSize)
    val sampleToSAX = tsToSAX(tsSample)

    println("numPart =  " + numPart)

    val partTree = new TerminalNode(Array.empty, config.zeroArray, config.zeroArray )
    sampleToSAX.collect.foreach{case (saxWord, tsId) => partTree.insert(saxWord, tsId)}
    val partTreeRoot  = partTree.split()

    0 until numPart-2 foreach { _ =>  partTreeSplit(partTreeRoot) } //TODO => if all are 0  -> break

    /** Partitioning TAble **/
    val partTable = partTreeRoot.partTable
    /** saves partTable to file 'workDir/partTable **/
    var writer = Utils.setWriter(fscopy, config.workDir + "partTable")
      partTable.foreach{case (nodeID, nodeCard, tsNum) => writer.write (nodeID + " " + nodeCard.mkString(",") + " " + tsNum + "\n") }
      writer.close

    /** saves partTree to file 'workDir/partTree' **/
    writer = Utils.setWriter(fscopy, config.workDir + "partTree.json")
      writer.write(partTreeRoot.toJSON(fscopy)); writer.close
    println("partTable:" ) ;partTable.map(v => (v._1, v._2.mkString("{",",","}"), v._3)).foreach(println)

    partTable
  }

  private def readPartTable()  = {
    /** reads partTable ( Array[ (String,Array[Int],Int)]) from file  'workDir/partTable' **/
    val fscopy = fsURI
    val partFromFile = Utils.setReader(fscopy, config.workDir + "partTable")
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
      val ((partNodeId, partCard), partId) = partRDD.value(first._1)
      val (wordToCard, nodeCard) = config.parseNodeId(partNodeId)

      val root = new InternalNode(partCard.map(_+1), mutable.HashMap.empty, partCard, wordToCard)
      root.insert(first._2._1, first._2._2)
      part.foreach{case (partID, (saxWord, tsId))  => root.insert(saxWord, tsId)}
      val writer = Utils.setWriter(fscopy, config.workDir + partRDD.value(first._1)._1._1 + ".json")
      writer.write(root.toJSON(fscopy)); writer.close
    }
  }

  private def approximateQuery(partRDD: PartTableBC, inputRDD: DataStatsRDD, queryRDD: DataStatsRDD) : ApproxRDD = {

    val querySAX = queryRDD.map( t => ( t._1, (t._2, config.tsToPAAandSAX(t._2)) ) )
    val querySAXpart = querySAX.map{case (tsId, (data, (paa, saxWord))) => (getPartID(saxWord, partRDD.value), (saxWord, tsId, paa, data))}
    val fscopy = fsURI

    val candidates =  querySAXpart.partitionBy(new HashPartitioner(numPart)).mapPartitions { part =>
      if (part.hasNext) {
        val first = part.next()
        val partNodeId = partRDD.value(first._1)._1._1
        val jsString = Utils.setReader(fscopy, config.workDir + partNodeId + ".json").mkString

        val json: JsValue = Json.parse(jsString)
        val root = deserJsValue(partNodeId, json, fscopy)

        var result = Iterator((first._2._2, first._2._1, root.approximateSearch(first._2._1, first._2._3), first._2._3, first._2._4))
        result ++= part.map { case (partID, (saxWord, queryId, paa, data)) => (queryId, saxWord, root.approximateSearch(saxWord, paa), paa, data) }
        result
      }
      else Iterator()
    }

    val approxRDD = candidates
      .flatMap{ case (qid, qw, tslist, paa, data) => tslist.map( t => (t, (qid, paa, data)) ) }
      .join(inputRDD)
      .map{case(tsid, ((q_id, q_paa, q_data), t_data)) => (q_id, (q_paa, q_data, tsid, t_data._1, config.distance(q_data, t_data)))}
      .combineByKey(v => (v._1, v._2, Array((v._3, v._4, v._5)))
        , (xs: CombineDataPAA, v) => (xs._1, xs._2, xs._3 :+ (v._3, v._4, v._5))
        , (xs: CombineDataPAA, ys: CombineDataPAA) => (xs._1, xs._2, xs._3 ++ ys._3))
      .map{ case(q_id, (q_paa, q_data, res)) => (q_id, q_paa, q_data, res.sortBy(_._3).take(config.topk)) }

    approxRDD
  }

  private def exactQuery(partRDD: PartTableBC, inputRDD: DataStatsRDD, approxRDD: ApproxRDD, sc: SparkContext) : OutputRDD = {

    val fscopy = fsURI

    val queryBC = sc.broadcast( approxRDD.map{ case (q_id, q_paa, q_data, res) => (q_id, (q_paa, res.last._3, q_data)) }.collectAsMap() )
    val partTableRDD = sc.parallelize(partRDD.value.map(v => (v._2,v._1)),numPart)

    val candidates = partTableRDD.flatMap{ case (part_id, (node_id, node_card)) =>
      val jsString = Utils.setReader(fscopy, config.workDir + node_id + ".json").mkString
      val json: JsValue = Json.parse(jsString)
      val root = deserJsValue(node_id, json, fscopy)
/*
      var tsMap = new mutable.HashMap[Long, mutable.ListBuffer[Long]]()

      queryBC.value.foreach { case (q_id, (q_paa, q_bound, q_data)) =>
          val it = root.boundedSearch(q_paa, q_bound, q_data._1.length)
          it.foreach(t => tsMap.getOrElseUpdate(t, new mutable.ListBuffer[Long]()) += q_id)
      }

      tsMap.iterator.map(t => (t._1, t._2.toArray))
*/
      queryBC.value.map{ case(q_id, (q_paa, q_bound, q_data)) => (q_id, root.boundedSearch(q_paa, q_bound, q_data._1.length))}

    }

    candidates.cache()
    println("Average candidates for exact search: " + candidates.map(_._2.length).sum() / queryBC.value.size)

    val exactRDD = candidates
      .flatMap{ case(qid, tslist) => tslist.map( t => (t, qid) )}
      .combineByKey(v => Array(v), (xs: Array[Long], v) => xs :+ v, (xs: Array[Long], ys: Array[Long]) => xs ++ ys)
      .join(inputRDD)
      .mapPartitions{ part =>
        var queryMap = new mutable.HashMap[Long, Array[(Long, Array[Float], Float)]]()
        queryBC.value.foreach(q => queryMap += (q._1 -> Array.empty))

        while (part.hasNext) {
          val (t_id, (q_list, t_data)) = part.next()
          q_list.foreach( q_id => queryMap(q_id) = mergeDistances(queryMap(q_id), Array((t_id, t_data._1, config.distance(queryBC.value(q_id)._3, t_data)))) )
        }

        queryMap.toIterator
      }
      .reduceByKey(mergeDistances)
      .map{ case(q_id, res) => (q_id, queryBC.value(q_id)._3._1, res)}

    exactRDD
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

    if (config.pls) {
      val plsRDD = linearSearch(inputRDD, queryRDD, sc)
      outputRDD("PLS", plsRDD)
    }

    numPart = config.numPart==0 match {
      case true  => sc.defaultParallelism//executors * coresPerEx
      case false => config.numPart
    }

    /*********************************/
    /** Indexing -  Distributed     **/
    /*********************************/

    /** Building partitioning tree  **/
    val start = System.currentTimeMillis()
    var buildIndexCond = true
  /** checks if partTable already exists for the given input, and skips building index, just reads table from file **/
    val fs = Utils.getFS(fsURI)

    val partTable =  fs.exists( new Path(config.workDir)) match {
      case true =>  buildIndexCond=false; readPartTable()
      case false => createPartTable(inputRDD)
    }

//   val partTable = createPartTable(inputRDD)
    val partRDD : PartTableBC = sc.broadcast(partTable.map(col => (col._1, col._2)).zipWithIndex)


    if (buildIndexCond) buildIndex(partRDD, inputRDD)

    val stop = System.currentTimeMillis()
    println("Indexing : " + (stop - start) + " ms (" +  Utils.getMinSec(stop-start) + ")" )

    /*********************************/
    /**  Approximate Search      **/
    /*********************************/


    val approxRDD = approximateQuery(partRDD, inputRDD, queryRDD)

    approxRDD.cache()

    outputRDD( "Approximate", approxRDD.map{ case (q_id, q_paa, q_data, res) => (q_id, q_data._1, res) } )


    /*********************************/
    /**  Exact Search               **/
    /*********************************/

//    val exactRDD = exactQuery(partRDD, inputRDD, approxRDD, sc.parallelize(0 until numPart))
    val exactRDD = exactQuery(partRDD, inputRDD, approxRDD, sc)
    outputRDD("Exact", exactRDD)

  }
}
