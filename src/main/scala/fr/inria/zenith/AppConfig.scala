package fr.inria.zenith

import com.typesafe.config.Config
import scala.math._
import org.apache.commons.math3.distribution.NormalDistribution

/**
  * Created by leva on 23/07/2018.
  */
case class AppConfig (config: Config) {

  val workDir =  "../dpisax_res/"

  val maxCardSymb = config.getInt("maxCardSymb.value")
  val wordLength: Integer = config.getInt("wordLength.value")

  val threshold = config.getInt("threshold.value")

  val topk = 10

//  val someOtherSetting = config.getString("some_other_setting")

  val breakpoints = initBreakpoints()

  private def initBreakpoints() : Array[Double] = {
    val nd = new NormalDistribution(0, 1)
    val mc = 1 << maxCardSymb
    (1 until mc).map( x => nd.inverseCumulativeProbability(x.toDouble / mc) ).toArray
  }

  def basicCard = Array.fill[Int](wordLength)(1)

  def nodeID(wordToCard: Array[Int], nodeCard: Array[Int]) : String  = (wordToCard zip nodeCard).map{case (w,c) => s"$w.$c"}.mkString("_")

  def basicSplitBalance (nodeCard: Array[Int]): Array[Array[Int]] = nodeCard.map(v => Array.fill[Int](maxCardSymb - v)(0))    //Array.fill[Array[Int]](wordLength)(Array.fill[Int](maxCardSymb)(0))

  def normalize(ts: Array[Float]) : Array[Float] = {
    val mean = ts.sum / ts.length
    val stdev = sqrt( ts.map(x => x * x).sum / ts.length - mean * mean ).toFloat
    ts.map( x => (x - mean) / stdev )
  }

  def tsToPAAandSAX(ts: Array[Float]) : (Array[Float], Array[Int]) = {
    // ts must be normalized
    val segmentSize = ts.length / wordLength
    val numExtraSegments = ts.length % wordLength
    val sliceBorder = (wordLength - numExtraSegments) * segmentSize
    val paa = (ts.slice(0, sliceBorder).sliding(segmentSize, segmentSize) ++ ts.slice(sliceBorder, ts.length).sliding(segmentSize+1, segmentSize+1)).map(t => t.sum / t.length).toArray

    val sax = paa.map(t => breakpoints.indexWhere(t <= _)).map(t => if (t == -1) breakpoints.length else t).toArray

    (paa, sax)
  }

  def tsToSAX(ts: Array[Float]) : Array[Int] = tsToPAAandSAX(ts)._2

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

  def distance(xs: Array[Float], ys: Array[Float]) : Float =
    sqrt((xs zip ys).map { case (x,y) => pow(y-x, 2)}.sum).toFloat

}
