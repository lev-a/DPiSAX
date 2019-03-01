package fr.inria.zenith

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by leva on 15/02/2019.
  */
object Utils {

  def getMinSec (millis : Long) =
    "%d min %d sec".format(TimeUnit.MILLISECONDS.toMinutes(millis), TimeUnit.MILLISECONDS.toSeconds(millis) -
      TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis)))

  def getFS (fsURI: String) = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", fsURI)
    FileSystem.get(conf)
  }

  def setWriter(fsURI: String, path: String) : PrintWriter = {

    val fs = getFS(fsURI)
    val output = fs.create(new Path(path))
    new PrintWriter(output)
  }

  def setReader(fsURI: String, path: String) : Array[String]  = {
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

  def getFileName(path: String) : String =
    new java.io.File(path).getName

}
