package org.example.demo1

import com.github.tototoshi.csv.CSVReader
import org.example.EmptyWriteTask
import org.grapheco.lynx.runner.{CypherRunner, GraphModel, WriteTask}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural._

import java.io.File

class Demo1 extends GraphModel {

  /**
  * STEP 1. Impl lynx elements.
  * LynxId, LynxNode, LynxRelationship
  * */
  private case class Id(value: String) extends LynxId {
    override def toLynxInteger: LynxInteger = ???
  }

  private case class Node(name: String) extends LynxNode{


    override val id: LynxId = Id(name)

    override def labels: Seq[LynxNodeLabel] = Seq.empty

    override def keys: Seq[LynxPropertyKey] = Seq(LynxPropertyKey("name"))

    override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = propertyKey match {
      case LynxPropertyKey("name") => Some(LynxValue(name))
      case _ => None
    }
  }

  private case class Relationship(source: String, target: String, weight: Int) extends LynxRelationship{

    override val id: LynxId = Id("")
    override val startNodeId: LynxId = Id(source)
    override val endNodeId: LynxId = Id(target)

    override def relationType: Option[LynxRelationshipType] = None
    override def keys: Seq[LynxPropertyKey] = Seq(LynxPropertyKey("weight"))

    override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = propertyKey match {
      case LynxPropertyKey("weight") => Some(LynxValue(weight))
      case _ => None
    }
  }

  /**
   * STEP 2. Load and convert data.
   * csv -> List[List[String]\] -> nodes and relationships
   */
  // Load CSV
  private val file = new File("/Users/huchuan/Documents/GitHub/Lynx_Demo/src/main/scala/org/example/demo1/game-of-thrones.csv")
  private val raw: List[List[String]] = CSVReader.open(file).all().tail

  // Covert to nodes and relationships
  private val allNodes: Map[Id, Node] = raw.flatMap(_.take(2)).distinct.map{ str => Id(str) -> Node(str)}.toMap

  private val allRelationships: List[Relationship] = raw.map(line => Relationship(line.head, line(1), line(2).toInt))

  /**
   * STEP 3. Impl three data methods.
   * nodeAt(), nodes() & relationships()
   */
  override def write: WriteTask = new EmptyWriteTask

  override def nodeAt(id: LynxId): Option[LynxNode] = id match {
    case i:Id => allNodes.get(i)
    case _ => None
  }

  override def nodes(): Iterator[LynxNode] = allNodes.valuesIterator

  override def relationships(): Iterator[PathTriple] =
    allRelationships.iterator.map(r => PathTriple(nodeAt(r.startNodeId).get, r, nodeAt(r.endNodeId).get))

  /**
   * STEP 4. put graphModel into runner
   * execute cypher by runner.run($query)
   */
  val runner = new CypherRunner(this)

}

object App {
  def main(args: Array[String]): Unit = {
    val demo = new Demo1
    demo.runner.run("Match (n) return n", Map.empty).show()
  }
}

