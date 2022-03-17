package ru.itis
package boolean_search


import scala.collection.SortedSet
import scala.io.Source

object Searcher {

  def main(args: Array[String]): Unit = {

    val file = Source.fromFile("/Users/Bulat_Saidashev/Desktop/projects/crawler/infopoisk/src/inverted_index.txt")
    val invertedIndex = file.getLines().toList
      .map(line => {
        val split = line.split(" ")
        split(0) -> split.tail.toList
      }).toMap
    file.close()

    val expression = args(0).split(" ")
    val operand = expression(1)
    val first = expression(0)
    val second = expression(2)


    operand match {
      case "AND" => {
        SortedSet[String](andOperand(invertedIndex, first, second).toList: _*)
          .foreach(println)
      }
      case "OR" => {
        SortedSet[String](andOperand(invertedIndex, first, second).toList: _*)
          .foreach(println)
      }
    }
  }


  def andOperand(invertedIndex: Map[String, List[String]], word1: String, word2: String): Set[String] = {
    (invertedIndex(word1), invertedIndex(word2)) match {
      case (list1: List[String], list2: List[String]) => return list1.intersect(list2).toSet
      case _ => return Set.empty
    }
  }

  def orOperand (invertedIndex: Map[String, List[String]], word1: String, word2: String) : Set[String] = {
    (invertedIndex(word1), invertedIndex(word2)) match {
      case (list1: List[String], list2: List[String]) =>  return list1.union(list2).toSet
      case _ =>  return Set.empty
  }

  }

}
