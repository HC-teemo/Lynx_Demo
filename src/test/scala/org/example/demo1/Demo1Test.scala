package org.example.demo1

import org.example.DemoRunner
import org.junit.jupiter.api.Test

@Test
class Demo1Test {
  val demo = new Demo1()
  val runner = new DemoRunner(demo)

  @Test
  def test1(): Unit = {
    runner.run("match (n) return n", Map.empty).show()
  }
}
