package fr.inria.zenith

import com.typesafe.config.Config
import org.apache.commons.math3.distribution.NormalDistribution

import scala.math._

/**
  * Created by leva on 23/07/2018.
  */
case class AppConfig (config: Config) {


  type TSWithStats = (Array[Float], (Float, Float))


  val maxCardSymb : Integer = config.getInt("maxCardSymb")
  val wordLength : Integer = config.getInt("wordLength")
  val threshold : Integer = config.getInt("threshold")
  val topk : Integer = config.getInt("topk")

  val tsFilePath : String = config.getString("tsFilePath")
  val queryFilePath : String = config.getString("queryFilePath")
  val firstCol : Integer = config.getInt("firstCol")

  val saveDir : String =  config.getString("saveDir") //TODO => saveDir ???
  val workDir : String = saveDir + Utils.getFileName(tsFilePath) + "_" + wordLength +  "_" + maxCardSymb + "/" //TODO => if tsFilePath is a full path /dir/dir/file


  val numPart : Integer = config.getInt("numPart")

  val sampleSize : Double = config.getDouble("sampleSize")

  val pls : Boolean = config.getBoolean("pls")

  val breakpoints : Array[Double] =
  {
    val nd = new NormalDistribution(0, 1)
    val mc = 1 << maxCardSymb
    (1 until mc).map( x => nd.inverseCumulativeProbability(x.toDouble / mc) ).toArray
  }

  def basicCard : Array[Int] = Array.fill[Int](wordLength)(1)

  def zeroArray : Array[Int] =  Array.fill[Int](wordLength)(0)

  def nodeID(wordToCard: Array[Int], nodeCard: Array[Int]) : String  = (wordToCard zip nodeCard).map{case (w,c) => s"$w.$c"}.mkString("_")

  def parseNodeId(nodeId: String) : (Array[Int], Array[Int]) =
    if (nodeId == "")
      (zeroArray, zeroArray)
    else
      nodeId.split("_").map(_.split("\\.")).map(p => (p(0).toInt, p(1).toInt)).unzip

  def basicSplitBalance (nodeCard: Array[Int]): Array[Array[Int]] = nodeCard.map(v => Array.fill[Int](maxCardSymb - v)(0))

  def stats(ts: Array[Float]) : (Float, Float) = {
    val mean = ts.sum / ts.length
    val stdev = sqrt( ts.map(x => x * x).sum / ts.length - mean * mean ).toFloat

    (mean, stdev)
  }

  def normalize(tsWithStats: TSWithStats) : Array[Float] =
    tsWithStats._1.map( x => if (tsWithStats._2._2 > 0) (x - tsWithStats._2._1) / tsWithStats._2._2 else 0.toFloat )

  def tsToPAAandSAX(tsWithStats: TSWithStats) : (Array[Float], Array[Int]) = {
    val ts = normalize(tsWithStats)
    val segmentSize = ts.length / wordLength
    val numExtraSegments = ts.length % wordLength
    val sliceBorder = (wordLength - numExtraSegments) * segmentSize
    val paa = (ts.slice(0, sliceBorder).sliding(segmentSize, segmentSize) ++ ts.slice(sliceBorder, ts.length).sliding(segmentSize+1, segmentSize+1)).map(t => t.sum / t.length).toArray

    val sax = paa.map(t => breakpoints.indexWhere(t <= _)).map(t => if (t == -1) breakpoints.length else t).toArray

    (paa, sax)
  }

  def tsToSAX(tsWithStats: TSWithStats) : Array[Int] = tsToPAAandSAX(tsWithStats)._2

  def mindist(paa: Array[Float], wordToCard: Array[Int], card: Array[Int], tsLength: Int) : Float = {
    val saxBounds = (wordToCard.iterator zip card.iterator).map{ case(w, c) => ( (w << (maxCardSymb - c)) - 1, ((w + 1) << (maxCardSymb - c)) - 1 )}

    val symDistSq = (paa.iterator zip saxBounds).map{ case(paaValue, (saxLower, saxUpper)) =>
      if (saxLower >= 0 && paaValue < breakpoints(saxLower))
        pow(paaValue - breakpoints(saxLower), 2).toFloat
      else if (saxUpper < breakpoints.length && paaValue > breakpoints(saxUpper))
        pow(paaValue - breakpoints(saxUpper), 2).toFloat
      else 0 }

    sqrt(symDistSq.sum / wordLength * tsLength).toFloat
  }

  def distance(xs: TSWithStats, ys: TSWithStats) : Float =
    sqrt((normalize(xs) zip normalize(ys)).map { case (x, y) => pow(y - x, 2)}.sum).toFloat



}
