//Scala-sdk-2.12.17
package org.example.demo2

import org.grapheco.lynx.runner._
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural._
import org.opencypher.v9_0.expressions.SemanticDirection
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import Schema._
import Mapper._
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput}

import java.sql.ResultSet

class Demo2 extends GraphModel {
  val connection = DB.connection

  override def write: WriteTask = new WriteTask {
    override def createElements[T](nodesInput: Seq[(String, NodeInput)], relationshipsInput: Seq[(String, RelationshipInput)], onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T = ???

    override def deleteRelations(ids: Iterator[LynxId]): Unit = ???

    override def deleteNodes(ids: Seq[LynxId]): Unit = ???

    override def updateNode(lynxId: LynxId, labels: Seq[LynxNodeLabel], props: Map[LynxPropertyKey, LynxValue]): Option[LynxNode] = ???

    override def updateRelationShip(lynxId: LynxId, props: Map[LynxPropertyKey, LynxValue]): Option[LynxRelationship] = ???

    override def setNodesProperties(nodeIds: Iterator[LynxId], data: Array[(LynxPropertyKey, Any)], cleanExistProperties: Boolean): Iterator[Option[LynxNode]] = ???

    override def setNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): Iterator[Option[LynxNode]] = ???

    override def setRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[(LynxPropertyKey, Any)]): Iterator[Option[LynxRelationship]] = ???

    override def setRelationshipsType(relationshipIds: Iterator[LynxId], typeName: LynxRelationshipType): Iterator[Option[LynxRelationship]] = ???

    override def removeNodesProperties(nodeIds: Iterator[LynxId], data: Array[LynxPropertyKey]): Iterator[Option[LynxNode]] = ???

    override def removeNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): Iterator[Option[LynxNode]] = ???

    override def removeRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[LynxPropertyKey]): Iterator[Option[LynxRelationship]] = ???

    override def removeRelationshipsType(relationshipIds: Iterator[LynxId], typeName: LynxRelationshipType): Iterator[Option[LynxRelationship]] = ???

    override def commit: Boolean = true
  }
  override def nodeAt(id: LynxId): Option[LynxNode] = ???

  private def singleTableSelect(tableName: String, filters: Map[LynxPropertyKey,LynxValue]): Iterator[ResultSet] = {
    val conditions: Map[String, Any] = filters.map { case (k,v) => k.toString -> v.value }
    singleTableSelect(tableName, conditions.toList)
  }

  private def singleTableSelect(tableName: String, conditions: Seq[(String, Any)]): Iterator[ResultSet] = {
    val selectWhat = nodeSchema.getOrElse(tableName, relSchema(tableName)).map(_._1).mkString(",")
    val sql = s"select $selectWhat from ${tableName} ${
      if (conditions.isEmpty) ""
      else " where " + conditions.map { case (key, value) => s"$key = '$value'" }.mkString(" and ")
    }"
    DB.iterExecute(sql)
  }

  private def nodeAt(id: LynxId, tableList: Seq[String]): Option[DemoNode] = {
    tableList.flatMap { t =>
      singleTableSelect(t, List((ID_COL_NAME, id.toLynxInteger.v)))
        .map(rs => Mapper.mapNode(rs, t, nodeSchema(t))).toSeq.headOption
    }.headOption
  }

  override def nodes(): Iterator[DemoNode] = nodeSchema.keys
    .toIterator.map(LynxNodeLabel).flatMap{ l =>
      nodes(NodeFilter(Seq(l), Map.empty))
    }

  override def nodes(nodeFilter: NodeFilter): Iterator[DemoNode] = {
    if (nodeFilter.labels.isEmpty && nodeFilter.properties.isEmpty) return nodes()
    val t = nodeFilter.labels.head.toString
    singleTableSelect(t, nodeFilter.properties).map { rs => Mapper.mapNode(rs, t, nodeSchema(t)) }
  }

  /**
   * Get all relationships in the database
   * @return Iterator[PathTriple]
   */
  override def relationships(): Iterator[PathTriple] = relSchema.keys
    .toIterator.map(LynxRelationshipType).flatMap { t =>
      relationships(RelationshipFilter(Seq(t), Map.empty))
    }

  /**
   * Get relationships with filter
   * @param relationshipFilter the filter with specific conditions
   * @return Iterator[PathTriple]
   */
  override def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] = {
    println("relationships: ", relationshipFilter)
    val t = relationshipFilter.types.head.toString
    singleTableSelect(t, relationshipFilter.properties).map{ rs =>
      PathTriple(
        nodeAt(DemoId(rs.getLong(START_ID_COL_NAME)), relMapping(t).source).get,
        Mapper.mapRel(rs, t, relSchema(t)),
        nodeAt(DemoId(rs.getLong(END_ID_COL_NAME)), relMapping(t).target).get
      )
    }
  }


  override def expand(nodeId: LynxId, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter, direction: SemanticDirection): Iterator[PathTriple] = {
    this.expand(nodeId, relationshipFilter,direction).filter { pathTriple =>
      endNodeFilter.matches(pathTriple.endNode)
    }
  }

  /**
   * Used for path like (A)-[B]->(C), where A is point to start expand, B is relationship, C is endpoint
   *
   * @param id id of A
   * @param filter relationship filter of B
   * @param direction OUTGOING (-[B]->) or INCOMING (<-[B]-)
   * @return Iterator[PathTriple]
   */
  override def expand(id: LynxId, filter: RelationshipFilter, direction: SemanticDirection): Iterator[PathTriple] = {
    if (direction == BOTH) { return expand(id, filter, OUTGOING) ++ expand(id, filter, INCOMING) }

    val tableName = filter.types.head.toString
    val relType = filter.types.head.toString
    val conditions = filter.properties.map { case (key, value) => s"`${key.toString}` = '${value.value}'" }.toArray

    val startNode = direction match {
      case OUTGOING => nodeAt(id, relMapping(tableName).source)
      case INCOMING => nodeAt(id, relMapping(tableName).target)
    }

    if (startNode.isEmpty) {
      return Iterator.empty
    }

    // start building sql query
    val selectWhat = relSchema(relType).map(_._1).mkString(",")
    val where = (direction match {
      case OUTGOING => s" where $relType.$START_ID_COL_NAME = ${id.toLynxInteger.value} "
      case INCOMING => s" where $relType.$END_ID_COL_NAME = ${id.toLynxInteger.value} "
    }) + (if(conditions.nonEmpty) " and " + conditions.mkString(" and ") else "")

    val sql = s"select $selectWhat from $relType $where"

    DB.iterExecute(sql).map { resultSet =>
      val endNode = direction match {
        case OUTGOING => nodeAt(DemoId(resultSet.getLong(END_ID_COL_NAME)), relMapping(tableName).target).get
        case INCOMING => nodeAt(DemoId(resultSet.getLong(START_ID_COL_NAME)), relMapping(tableName).source).get
      }
      PathTriple(startNode.get, mapRel(resultSet, relType, relSchema(relType)), endNode)
    }
  }

  /**
   * Used for path like (A)-[B]->(C), but here A may be more than one node
   * Allow multi-hop queries like (A)-[B*0..]->(C)
   * @param startNodeFilter node filter of A
   * @param relationshipFilter relationship filter of B
   * @param endNodeFilter node filter of C
   * @param direction OUTGOING (-[B]->) or INCOMING (<-[B]-)
   * @param upperLimit maximum length of path
   * @param lowerLimit minimum length of path
   * @return Iterator[LynxPath]
   */
//  override def paths(startNodeFilter: NodeFilter, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter,
//                     direction: SemanticDirection, upperLimit: Int, lowerLimit: Int): Iterator[LynxPath] = {
//
//    if (upperLimit != 1 || lowerLimit != 1) throw new RuntimeException("Upper limit or lower limit not support")
//
//    if (direction == BOTH) {
//      return paths(startNodeFilter, relationshipFilter, endNodeFilter, OUTGOING, upperLimit, lowerLimit) ++
//        paths(startNodeFilter, relationshipFilter, endNodeFilter, INCOMING, upperLimit, lowerLimit)
//    }
//
//    val relType = relationshipFilter.types.head.toString
//    val startTables = if (startNodeFilter.labels.nonEmpty) {startNodeFilter.labels.map(_.toString).toArray}
//                      else {relMapping(relType)._1}
//    val endTables = if (endNodeFilter.labels.nonEmpty) {endNodeFilter.labels.map(_.toString).toArray}
//                    else {relMapping(relType)._2}
//    val conditions = Array.concat(
//      startNodeFilter.properties.map { case (key, value) => s"t1.`${key.toString}` = '${value.value}'" }.toArray,
//      relationshipFilter.properties.map { case (key, value) => s"$relType.`${key.toString}` = '${value.value}'" }.toArray,
//      endNodeFilter.properties.map { case (key, value) => s"t2.`${key.toString}` = '${value.value}'" }.toArray
//    )
//
//    // iterate through all possible combinations
//    val finalResult = for {
//      startTable: String <- startTables
//      endTable: String <- endTables
//    } yield {
//      var sql = s"select * from $startTable as t1 join $relType on t1.$ID_COL_NAME = "
//      direction match {
//        case OUTGOING => sql = sql + s"$relType.$START_ID_COL_NAME join $endTable as t2 on t2.$ID_COL_NAME = $relType.$END_ID_COL_NAME"
//        case INCOMING => sql = sql + s"$relType.$END_ID_COL_NAME join $endTable as t2 on t2.$ID_COL_NAME = $relType.$START_ID_COL_NAME"
//      }
//
//      for (i <- conditions.indices) {
//        if (i == 0) {
//          sql = sql + " where " + conditions(i)
//        } else {
//          sql = sql + " and " + conditions(i)
//        }
//      }
//
//      DB.iterExecute(sql).map{ resultSet =>
//        PathTriple(
//          mapNode(resultSet, startTable, nodeSchema(startTable)),
//          mapRel(resultSet, relType, relSchema(relType)),
//          mapNode(resultSet, endTable, nodeSchema(endTable))
//        ).toLynxPath
//      }
//    }
//
//    finalResult.iterator.flatten
//  }

// 原来的paths()
//override def paths(startNodeFilter: NodeFilter, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter,
//            direction: SemanticDirection, upperLimit: Int, lowerLimit: Int): Iterator[LynxPath] = {
    // println("paths()" + upperLimit + " " + lowerLimit)
    // 先不考虑多跳的情况
//    if (upperLimit != 1 || lowerLimit != 1) {
//      throw new RuntimeException("Upper limit or lower limit not support")
//    }

//    if (startNodeFilter.properties.size == 0) {
//      val result = relationships(relationshipFilter).map(_.toLynxPath)
//                      .filter(_.endNode.forall(endNodeFilter.matches))
//      println("paths() finished 2")
//      return result
//    }

//    val originStations = nodes(startNodeFilter)
//    val result = originStations.flatMap { originStation =>
//       val firstStop = expandNonStop(originStation, relationshipFilter, direction, lowerLimit)
//       val leftSteps = Math.min(upperLimit, 100) - lowerLimit
//       firstStop.flatMap(p => extendPath(p, relationshipFilter, direction, leftSteps))
//      // expandNonStop(originStation, relationshipFilter, direction, lowerLimit)
//    }.filter(_.endNode.forall(endNodeFilter.matches))

    // println("paths() finished 1")
//    result
//  }

}


