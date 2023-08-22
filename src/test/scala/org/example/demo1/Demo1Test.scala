package org.example.demo1

import org.junit.jupiter.api.Test

@Test
class Demo1Test {
  val demo = new Demo1()

  @Test
  def test1(): Unit = {
    demo.runner.run("match (n) return n", Map.empty).show()
  }
}
