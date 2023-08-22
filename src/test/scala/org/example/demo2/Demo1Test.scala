package org.example.demo2

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import doobie.implicits._
import doobie.util.transactor.Transactor
import org.grapheco.lynx.runner.{CypherRunner, NodeFilter}
import org.grapheco.lynx.types.structural.LynxNodeLabel
import org.junit.jupiter.api.Test

@Test
class Demo1Test {

  @Test
  def graphTest(): Unit = {
    val demo = new MyGraph
    val runner: CypherRunner = new CypherRunner(demo)
    runner.run("match (n:Person) where n.firstName = 'Hossein' return count(n)", Map.empty).show()
  }

  @Test
  def ldbcTest(): Unit = {
    val IS3 =
      """
        |MATCH (n:Person {id: 313194139537358 })-[r:KNOWS]-(friend)
        |RETURN
        |    friend.id AS personId,
        |    friend.firstName AS firstName,
        |    friend.lastName AS lastName,
        |    r.creationDate AS friendshipCreationDate
        |ORDER BY
        |    friendshipCreationDate DESC,
        |    toInteger(personId) ASC
        |""".stripMargin
    val demo = new MyGraph
    val runner: CypherRunner = new CypherRunner(demo)
    runner.run(IS3, Map.empty).show(100)
  }

  @Test
  def test(): Unit = {
    val demo = new MyGraph
    val runner: CypherRunner = new CypherRunner(demo)
    runner.run(
    """match (p)-[KNOWS]-(friend)-[published]-(paper)
        |where p.id = 1234
        |return friend.name, count(paper)
        |""".stripMargin
    , Map.empty).show(100)
  }
}
