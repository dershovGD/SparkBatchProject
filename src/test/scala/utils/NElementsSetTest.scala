package utils

import org.scalatest.FunSuite

class NElementsSetTest extends FunSuite {

  test("testAdd") {
    val expected = Array(2,4)

    val set = new NElementsSet[Int](2, Ordering.Int)
    set += 1
    set += 2
    set += 4
    set += -1
    assert(set.toList.toArray === expected)
  }

  test("testMerge") {
    val expected = Array(2,4)
    val set1 = new NElementsSet[Int](2, Ordering.Int)
    val set2 = new NElementsSet[Int](3, Ordering.Int)
    set1 += 1
    set1 += 2

    set2 += -1
    set2 += 4

    set1 ++= set2
    assert(set1.toList.toArray === expected)

  }

  test("testListWithKeyCreation") {
    val expected = List(("a", 1), ("a", 2))
    val set1 = new NElementsSet[Int](2, Ordering.Int)
    set1 += 1
    set1 += 2

    val actual = set1.listWithKey("a")
    assert(actual === expected)
  }

}
