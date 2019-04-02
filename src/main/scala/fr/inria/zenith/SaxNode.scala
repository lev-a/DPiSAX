package fr.inria.zenith

/**
  * Created by leva on 20/07/2018.
  */
abstract class SaxNode(val nodeCard: Array[Int], val wordToCard: Array[Int]) {

  def insert(saxWord: Array[Int] , tsId: Long )

  def shallSplit : Boolean

  def split () : SaxNode

  def approximateSearch(saxWord: Array[Int], paa: Array[Float]) : Array[Long]

  def boundedSearch(paa: Array[Float], bound: Float, tsLength: Int) : Array[Long]

  def fullSearch : Array[Long]

  def toJSON (fsURI: String) : String

  def partTreeSplit (node: String) : Unit

  def partTable : Array[(String,Array[Int],Int)]

}
