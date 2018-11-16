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
    return this
  }

  def toList: List[T] = {
    return new util.ArrayList[T](queue).asScala.toList
  }

  def ++=(anotherSet: NElementsSet[T]): NElementsSet[T] = {
    for (e <- anotherSet.toList) {
      this += e
    }
    return this
  }

  override def toString = queue.toString

}
