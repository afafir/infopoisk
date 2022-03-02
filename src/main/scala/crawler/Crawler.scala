package ru.itis
package crawler


import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import scala.language.postfixOps

object Crawler{


  val baseUrl = "https://vc.ru/"

  def main(args: Array[String])
  {
    val executors = Executors.newWorkStealingPool(8)
    val lock = new ReentrantLock()
    (100 to 200)
      .map(x => executors.submit(new Downloader(baseUrl.concat(String.valueOf(x)), f"src/res/$x.txt", lock)))
      .foreach(x => x.get())

  }


}
