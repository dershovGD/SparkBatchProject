package utils

import java.io.Serializable
import java.util
import java.util.{Comparator, PriorityQueue}

import scala.collection.JavaConverters._


class NElementsSet[T](private val n: Int, private val comparator: Comparator[T]) extends Serializable {
  private val queue = new PriorityQueue[T](comparator)

  def +=(elem: T): NElementsSet[T] = {
    if (queue.size < n) {
      queue.add(elem)
    } else {
      if (comparator.compare(elem, queue.peek()) > 0) {
        queue.poll()
        queue.add(elem)
      }
    }
    this
  }

  def toList: List[T] = {
    new util.ArrayList[T](queue).asScala.toList
  }

  def listWithKey[K](key: K): List[(K, T)] = {
    toList.map((key, _))
  }

  def ++=(anotherSet: NElementsSet[T]): NElementsSet[T] = {
    for (e <- anotherSet.toList) {
      this += e
    }
    this
  }

  override def toString = queue.toString

}
