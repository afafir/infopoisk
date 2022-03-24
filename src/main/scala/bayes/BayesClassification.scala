package ru.itis
package bayes

import java.io.FileWriter
import scala.collection.mutable.ListBuffer
import scala.io.Source

object BayesClassification {

  val tokensPath = "/Users/Bulat_Saidashev/Desktop/projects/crawler/infopoisk/src/token"
  val lemmasPath = "/Users/Bulat_Saidashev/Desktop/projects/crawler/infopoisk/src/lemma/"

  def main(args: Array[String]): Unit = {

    val tokens = ListBuffer[Document]()
    val lemmas = ListBuffer[Document]()
    (100 to 200)
      .foreach(num => {
        tokens += Document(num, s"$tokensPath/tokens_$num.txt")
        lemmas += Document(num, s"$lemmasPath/lemmas_$num.txt")
      })

    val tokensParsed = readAllDocuments(tokens.toList)
    val lemmasParsed = parseLemmas(readAllDocuments(lemmas.toList))

    val documentWordCount = tokensParsed.map(
      x => (x._1 ,x._2.size)
    )
    val  tokensCountInEachDocument :  Map[Int, Map[String, Int]] = tokensParsed
      .mapValues(tokens => tokens.groupBy(identity).mapValues(_.size))



    val tokensResult = tokensCountInEachDocument.par.map(pair => {
      val bayesResults = ListBuffer[BayesResult]()
      for ((word, count) <- pair._2) {
        val wordFrequencyInAllDocuments = tokensCountInEachDocument.foldLeft(0)((acc, pair) => {
          if (pair._2.contains(word)) acc+1 else acc
        })
        val idf = scala.math.log(tokensCountInEachDocument.size / wordFrequencyInAllDocuments)
        bayesResults += BayesResult(word = word, idf = idf, tfIdf = (idf.toDouble * count.toDouble) / documentWordCount(pair._1).toDouble)
      }
      println(s"${pair._1} tokens processed")
      (pair._1, bayesResults)
    })

    val lemmasResult = lemmasParsed.map(pair => {
      val bayesResults = ListBuffer[BayesResult]()
      for ((lemma, tokens) <- pair._2) {
        val count = tokens.foldLeft(0)((acc, token) => {
          acc + tokensCountInEachDocument(pair._1)(token)
        })
        val tf = (count.toDouble / documentWordCount(pair._1).toDouble)
        println(s"${pair._1} - $count - $lemma - $tf")
        val lemmaFrequencyInAllDocuments = lemmasParsed.foldLeft(0)((acc, pair1) => {
          if (pair1._2.contains(lemma)) acc+1 else acc
        })
        val idf = scala.math.log(lemmasParsed.size / lemmaFrequencyInAllDocuments)
        bayesResults += BayesResult(word = lemma, idf = idf, tfIdf = tf * idf)
      }
      (pair._1, bayesResults)
    })

    tokensResult.par.foreach(pair => {
      val fw = new FileWriter("/Users/Bulat_Saidashev/Desktop/projects/crawler/infopoisk/bayes/lemma/token_bayes_".concat(pair._1.toString + ".txt"), true)
      for (result <- pair._2) {
        fw.write(s"${result.word} ${result.idf} ${result.tfIdf}")
      }
      fw.write("\n")
      fw.flush()
      fw.close()
    })

    lemmasResult.par.foreach(pair => {
      val fw = new FileWriter("/Users/Bulat_Saidashev/Desktop/projects/crawler/infopoisk/bayes/lemma/lemma_bayes_".concat(pair._1.toString + ".txt"), true)
      for (result <- pair._2) {
        fw.write(s"${result.word} ${result.idf} ${result.tfIdf}")
      }
      fw.write("\n")
      fw.flush()
      fw.close()
    })
    }


  def readLines(path: String) : List[String] = {
    val file = Source.fromFile(path)
    try {
      file.getLines().toList
    } finally {
      file.close()
    }
  }

  def readAllDocuments(documents : List[Document]) : Map[Int, List[String]] = {
    documents.par
      .map(document => document.documentNum -> readLines(document.path))
      .seq
      .toMap
  }

  def parseLemmas(documents : Map[Int, List[String]]): Map[Int, Map[String, List[String]]] = {
    documents.map(pair => {
      val lemmaTokens : Map[String, List[String]] = pair._2
        .map(line => {
          val lemma = line.substring(0, line.indexOf(':'))
          val tokens = line.substring(line.indexOf(':') + 2).split(" ").toList
          (lemma, tokens)
        }).toMap
      (pair._1, lemmaTokens)
    })
  }

  case class BayesResult(word: String,
                          idf: Double,
                         tfIdf: Double)

  case class Document(documentNum: Int,
                      path: String)

}
