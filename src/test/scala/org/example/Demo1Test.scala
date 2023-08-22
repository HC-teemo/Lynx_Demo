package org.example
import doobie._
import doobie.implicits._
import cats.effect.IO
import cats.effect.unsafe.implicits.global

import scala.concurrent.ExecutionContext
import doobie.util.transactor.Transactor
import org.example.demo2.Demo2
import org.grapheco.lynx.runner.{CypherRunner, NodeFilter}
import org.grapheco.lynx.types.structural.LynxNodeLabel
import org.junit.jupiter.api.Test

@Test
class Demo1Test {

  @Test
  def dbTest(): Unit = {
    val xa = Transactor.fromDriverManager[IO](
      "com.mysql.cj.jdbc.Driver", // driver classname
      "jdbc:mysql://10.0.82.144:3306/LDBC1?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false", // connect URL (driver-specific)
      "root", // user
      "Hc1478963!" // password
    )
    val a = (sql"select * from Comment limit 2".query[Int].unique).transact(xa).unsafeRunSync()
    println(a)
  }

  @Test
  def demoTest(): Unit = {
    val demo = new Demo2
    demo.nodes(NodeFilter(Seq(LynxNodeLabel("Person")), Map.empty)).take(5).foreach(println)
  }

  @Test
  def graphTest(): Unit = {
    val demo = new Demo2
    val runner: CypherRunner = new CypherRunner(demo)
    runner.run("match (n:Person) where n.firstName = 'Hossein' return n limit 10", Map.empty).show()
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
    val demo = new Demo2
    val runner: CypherRunner = new CypherRunner(demo)
    runner.run(IS3, Map.empty).show(100)
  }

  @Test
  def test(): Unit = {
    val demo = new Demo2
    val runner: CypherRunner = new CypherRunner(demo)
    runner.run(
    """match (p)-[KNOWS]-(friend)-[published]-(paper)
        |where p.id = 1234
        |return friend.name, count(paper)
        |""".stripMargin
    , Map.empty).show(100)
  }
}
