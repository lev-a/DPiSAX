package fr.inria.zenith

import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.io.Source
import scala.math.sqrt

/**
  * Created by leva on 20/12/2018.
  * Centralised version
  */

object iSAX  {

  val config = AppConfig(ConfigFactory.load())
  def zeroArray =  Array.fill[Int](config.wordLength)(0)


  private def tsToSAX(ts: Iterator[(Int, Array[Float])]): Iterator[(Array[Int],Int)] = {
    // normalization
    val tsWithStats = ts.map(t => (t._1, t._2, t._2.sum / t._2.length, t._2.map(x => x * x).sum / t._2.length)).map(t => (t._1, t._2, t._3, sqrt(t._4 - t._3 * t._3).toFloat))
    val tsNorm = tsWithStats.map(t => (t._1, t._2.map(x => (x - t._3) / t._4)))

    //PAA
    val tsLength = ts.next._2.length
    val segmentSize = tsLength / config.wordLength
    val numExtraSegments = tsLength % config.wordLength
    val sliceBorder = (config.wordLength - numExtraSegments)*segmentSize
    val tsSegments = tsNorm.map(ts => ((ts._2.slice(0,sliceBorder).sliding(segmentSize, segmentSize) ++ ts._2.slice(sliceBorder,tsLength).sliding(segmentSize+1, segmentSize+1)).map(t => t.sum / t.length), ts._1))

  //  val segmentSize = ts.next._2.length / config.wordLength
  //  val tsSegments = tsNorm.map(ts => (ts._2.sliding(segmentSize, segmentSize).map(t => t.sum / t.length), ts._1))
    tsSegments.map(ts => (ts._1.map(t => config.breakpoints.indexWhere(t <= _)).map(t => if (t == -1) config.breakpoints.length else t).toArray, ts._2))
  }


  def main(args: Array[String]): Unit = {


    //val filename = "/Users/leva/Downloads/ts_1000"
    val tsFilePath = "./datasets/Seismic_data.data.csv" //TODO parameter
    val financeData = "./datasets/Finance_Data.csv"

    /** Indexing - Centralized  **/
    val tsInput = Source.fromFile(tsFilePath).getLines().map(_.split(",")).map(t => (t(0).toInt, t.tail.map(_.toFloat)))
    val financeInput = Source.fromFile(financeData).getLines().map(_.split(",")).map(t => (t(0).toInt, t.tail.tail.map(_.toFloat)))

        val SAXword = tsToSAX(tsInput)
        //println(SAXword.next.size)
        //SAXword.map(v => (v._2, v._1.mkString("{",",","}"))).foreach(println(_))

        val root = new InternalNode(config.basicCard, mutable.HashMap.empty)
        SAXword.foreach{case (saxWord, tsId) => root.insert(saxWord, tsId)} //returns nothing

 //      println (root.toJSON) /** Prints tree to JSON **/


    /*********************************/
    /**     Querying                **/
    /*********************************/


    /** Querying - Centralized  **/


    //val queryfile = "/Users/leva/Downloads/query_test.txt" //TODO parameter
    val queryfile = "./datasets/seismic_query_10.txt"
    //val queryfile = "/Users/leva/Downloads/query_10"
    val queryInput = Source.fromFile(queryfile).getLines().map(_.split(",")).map(t => (t(0).toInt, t.tail.map(_.toFloat)))
    val querySAXword = tsToSAX(queryInput)
     //querySAXword.map(v => (v._2, v._1.mkString("{", ",", "}"))).foreach(println(_))

    /** Approximate search **/

    val result = querySAXword.map(query => (query._2,query._1, root.approximateSearch(query._1))) // tuple of (Q_id, Q_word, List_of_tsIDs: Array[(Array[Int],Int)] )

    //  result.map { case (qid, tslist) => (qid,  tslist.map { case (tsw, tsid) => (tsw.mkString("<", ",", ">"), tsid) }.mkString) }.foreach(println(_))
  //  result.map { case (qid, qw, tslist) => (qid, qw.mkString("<", ",", ">"), tslist.map { case (tsw, tsid) => (tsw.mkString("<", ",", ">"), tsid) }.mkString) }.foreach(println(_))
    result.map { case (qid, qw, tslist) => (qid,  tslist.map { case (tsw, tsid) =>  tsid }.mkString(","), tslist.length) }.foreach(println(_))



    //TODO Exact Search
    // get Terminal node by Approximate  search
    // Traverse the tree and compare PAA to 10th top result of the approximate search

  }

}