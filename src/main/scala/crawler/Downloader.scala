package ru.itis
package crawler

import java.io.{File, FileWriter}
import java.net.URL
import java.util.concurrent.locks.ReentrantLock
import scala.language.postfixOps
import sys.process._

class Downloader(url: String, path: String, reentrantLock: ReentrantLock) extends Runnable{

  def run(): Unit = {
    val urlObject = new URL(url)
    urlObject #> new File(path) !!

    reentrantLock.lock()
    val fw = new FileWriter("index.txt", true)
    try {
      fw.write(f"$path - $url \n")
    }
    finally fw.close()
    reentrantLock.unlock()
    //to escape 429 HTTP
    Thread.sleep(1000)
  }
}
