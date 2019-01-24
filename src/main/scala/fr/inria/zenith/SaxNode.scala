package fr.inria.zenith

/**
  * Created by leva on 20/07/2018.
  */
abstract class SaxNode {

  def insert(saxWord: Array[Int] , tsId: Int )

  def shallSplit : Boolean

  def split () : SaxNode

  def approximateSearch(saxWord: Array[Int]) : Array[(Array[Int],Int)]

  def boundedSearch(paa: Array[Float], bound: Float, tsLength: Int) : Array[(Array[Int],Int)]

  def toJSON : String

  def partTreeSplit (node: String) : Unit

  def partTable : Array[(String,Array[Int],Int)]

}
