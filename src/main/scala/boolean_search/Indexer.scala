package ru.itis
package boolean_search

import java.io.FileWriter
import scala.io.Source

object Indexer {

  val lemmas_folder = "/Users/Bulat_Saidashev/Desktop/projects/crawler/infopoisk/src/lemma/"

  def main(args: Array[String]): Unit = {
    var map = Map[String, List[String]]()

    (100 to 200)
      .foreach(x => {
        val file = Source.fromFile(lemmas_folder.concat(s"lemmas_$x.txt"))
        val lemmas = file.getLines()
          .map(line => line.substring(0, line.indexOf(":")))
          .toList
        map += (x.toString -> lemmas)
        file.close()
      })

    var invertedIndex = Map[String, List[String]]()

    for ((num, lemmas) <- map) {
      lemmas.foreach(lemma => {
        invertedIndex get lemma match {
          case None => invertedIndex += (lemma -> List.apply(num))
          case Some(list) => invertedIndex += (lemma -> (list :+ num).sorted)
        }
      })
    }



    invertedIndex.foreach(println)

    for ((lemma, nums) <- invertedIndex) {
      val fw = new FileWriter("/Users/Bulat_Saidashev/Desktop/projects/crawler/infopoisk/src/inverted_index.txt", true)
      fw.write(s"$lemma ")
      nums.foreach(num => {
        fw.write(s"$num ")
      })
      fw.write("\n")
      fw.flush()
      fw.close()
    }



  }

}
