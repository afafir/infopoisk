package ru.itis
package demo

import com.github.demidko.aot.WordformMeaning
import ru.itis.boolean_search.Searcher

import scala.collection.mutable
import scala.io.Source

object Task {

  val idfFolder:String = "src/resources/bayes/lemma/"

  val filename_tfIdfs: mutable.Map[String, Map[String, (Double, Double)]]
          = mutable.Map[String, Map[String, (Double, Double)]]()
  val lemmas_idf: mutable.Map[String, Double] = mutable.Map[String, Double]()

  def init(words : List[String]) : Unit = {
    val query = words.mkString(" OR ")
    println(query)
    val set = Searcher.search(query)
    set.foreach( filename => {
      val file = Source.fromFile(idfFolder.concat(s"lemma_bayes_$filename.txt"))
      val tfIdfs = file.getLines().toList
        .map(line => {
          val split = line.split(" ")
          val word = split(0)
          val idf = split(1)
          val tf_idf = split(2)
          lemmas_idf.put(word, idf.toDouble)
          word -> (idf.toDouble, tf_idf.toDouble)
        }).toMap
      file.close()
      filename_tfIdfs.put(filename, tfIdfs)
    }
    )
  }

  def vectorSearch(queryLemmas : List[String]) : Unit = {
    val uniqueLemmas = lemmas_idf.keySet
    val docVectors = filename_tfIdfs.toMap.map(pair => {
      val vector = uniqueLemmas.toList.map(lemma => {
        if(pair._2.contains(lemma)) {
           pair._2(lemma)._2
        } else  0
      })
      (pair._1, vector)
    })
    val queryVector = uniqueLemmas.toList.map(lemma => {
      if (queryLemmas.contains(lemma)){
        val lemCount = queryLemmas.foldLeft(0)((acc, currLem) => {
          if (lemma.equals(currLem)) acc+1
          else acc
        })
        val lemmaTf = lemCount.toDouble / queryLemmas.size.toDouble
        val lemmaTfIdf = lemmaTf * lemmas_idf(lemma)
        lemmaTfIdf
      } else 0
    })

    val result = docVectors.map(
      docVector => (docVector._1, similarity(docVector._2, queryVector), docVector._2)
    ).toSeq.sortWith(_._2 > _._2)

    result.foreach(println)
    }

  def similarity(a : List[Double], b: List[Double]): Double = {

    val scalar = a.zip(b).foldLeft(0.toDouble)((acc, pair) => {
      acc + pair._1 * pair._2
    })

    val aNorm = math.sqrt(a.map(x => math.pow(x, 2)).sum)
    val bNorm = math.sqrt(b.map(x => math.pow(x, 2)).sum)
    scalar / (aNorm * bNorm)
  }

  def main(args: Array[String]): Unit = {
    val lemmasQuery = args.map(
      word => WordformMeaning.lookupForMeanings(word).get(0).getLemma.toString
    )
    init(lemmasQuery.toList)
    vectorSearch(lemmasQuery.toList)
  }

}
