package ru.itis
package tokenizer

import com.johnsnowlabs.nlp.{DocumentAssembler, SparkNLP}
import com.johnsnowlabs.nlp.annotator.{LemmatizerModel, Normalizer, Tokenizer}
import com.johnsnowlabs.nlp.annotators.StopWordsCleaner
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{arrays_zip, collect_list, explode, size, struct}
import org.jsoup.Jsoup

import java.io.FileWriter



object Tokenizer {

  def main(args: Array[String]): Unit = {
    val spark = SparkNLP.start(gpu = false, spark23 = false)
    import spark.implicits._
    val df = spark.sparkContext
      .wholeTextFiles("/Users/Bulat_Saidashev/Desktop/projects/crawler/infopoisk/src/res/*")
      .map(row => (row._1.substring(row._1.lastIndexOf("/") + 1), Jsoup.parse(row._2).body().text()))
      .toDF("filename", "text")

    val documentAssembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val tokenizer = new Tokenizer()
      .setInputCols(Array("document"))
      .setOutputCol("token")

    val normalizer = new Normalizer()
      .setInputCols(Array("token"))
      .setOutputCol("normalized")
      .setCleanupPatterns(Array("[^\\u0400-\\u04FF]"))
      .setLowercase(true)

    val stop_words = StopWordsCleaner.pretrained("stopwords_iso","ru")
      .setInputCols(Array("normalized"))
      .setOutputCol("cleanTokens")

    val lemmatizer = LemmatizerModel.pretrained("lemma_spacylookup","ru")
      .setInputCols(Array("cleanTokens"))
      .setOutputCol("lemma")


    val pipeline = new Pipeline().setStages(Array(documentAssembler, tokenizer, normalizer, stop_words, lemmatizer))
    val results = pipeline.fit(df).transform(df)
    val lemmasTokens = results
      .select($"filename",$"lemma.result".as("lemma"), $"cleanTokens.result".as("token"))
      .withColumn("tmp", arrays_zip($"lemma", $"token"))
      .withColumn("tmp", explode($"tmp"))
      .select("filename", "tmp.lemma", "tmp.token")

    val lemmas = lemmasTokens.groupBy($"filename", $"lemma")
      .agg(collect_list("token").as("tokens"))
      .select($"filename", struct($"lemma", $"tokens").as("groupedLemmas"))
      .groupBy($"filename")
      .agg(collect_list("groupedLemmas").as("lemmaTokens"))
      .as[LemmasGroupedRecord]

    lemmas.foreach(row => {
      val fw = new FileWriter("/Users/Bulat_Saidashev/Desktop/projects/crawler/infopoisk/src/lemma/lemmas_".concat(row.filename), true)
      row.lemmaTokens.foreach(lemmaTokens => {
        fw.write(s"${lemmaTokens.lemma}: ")
        val uniqueTokens = lemmaTokens.tokens.toSet
        uniqueTokens.foreach(token => {
          fw.write(s"$token ")
        })
        fw.write("\n")
      })
    })



    val tokens = lemmasTokens.select($"filename", $"token")
      .groupBy($"filename")
      .agg(collect_list("token").as("tokens"))
      .as[TokenRecord]

    tokens.foreach(row => {
      val fw = new FileWriter("/Users/Bulat_Saidashev/Desktop/projects/crawler/infopoisk/src/token/tokens_".concat(row.filename), true)
      row.tokens.foreach(token => {
        println(token)
      })
    })

  }

  case class TokenRecord( filename: String,
                          tokens: List[String])

  case class LemmasGroupedRecord( filename: String,
                                  lemmaTokens: List[LemmaTokens])

  case class LemmaTokens(lemma: String,
                         tokens: List[String])
}
