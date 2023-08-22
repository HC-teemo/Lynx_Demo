//Scala-sdk-2.12.17
package org.example.demo2

import org.grapheco.lynx.LynxResult
import org.grapheco.lynx.runner._
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property._
import org.grapheco.lynx.types.structural._
import org.grapheco.lynx.types.time.LynxDate
import org.opencypher.v9_0.expressions.SemanticDirection
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

import java.sql.{Connection, DriverManager, ResultSet}
import java.time.LocalDate
import java.util.Date
import Schema._

class MyGraph extends GraphModel {
  val connection = DB.connection

  override def write: WriteTask = ???
  override def nodeAt(id: LynxId): Option[LynxNode] = ???

  private def DemoNodeAt(id: LynxId, tableList: List[String]): Option[DemoNode] = {
    tableList.flatMap { t =>
        val sql = s"select * from $t where `id:ID` = ${id.toLynxInteger.value}"
        DB.iterExecute(sql).map(rs => Mapper.mapNode(rs, t, Schema.nodeSchema(t))).toSeq.headOption
    }.headOption
  }


  override def nodes(): Iterator[DemoNode] = {
    // iterate all node tables
    val allNodes = for (tableName <- nodeSchema.keys) yield {
      val statement = connection.createStatement
      val data = statement.executeQuery(s"select * from $tableName")

      // transform all rows in the table to LynxNode
      Iterator.continually(data).takeWhile(_.next())
        .map { resultSet => rowToNode(resultSet, tableName, nodeSchema(tableName)) }
    }

    println("nodes() finished")
    allNodes.flatten.iterator
  }

  override def nodes(nodeFilter: NodeFilter): Iterator[DemoNode] = {

    if (nodeFilter.labels.isEmpty && nodeFilter.properties.isEmpty) {
      return nodes()
    }

    val tableName = nodeFilter.labels.head.toString
    val conditions = nodeFilter.properties.map { case (key, value) => s"`${key.toString}` = '${value.value}'" }.toArray

    var sql = "select * from " + tableName

    // add conditions to the sql query
    for (i <- conditions.indices) {
      if (i == 0) {
        sql = sql + " where " + conditions(i)
      } else {
        sql = sql + " and " + conditions(i)
      }
    }

    println(sql)

    val statement = connection.createStatement
    val startTime2 = System.currentTimeMillis()
    val data = statement.executeQuery(sql)
    println("nodes(nodeFilter) SQL used: " + (System.currentTimeMillis() - startTime2) + " ms")

    // transform the rows in the sql result to LynxNodes
    val result = Iterator.continually(data).takeWhile(_.next())
      .map { resultSet => rowToNode(resultSet, tableName, nodeSchema(tableName)) }

    result
  }
//
//  /**
//   * Get all relationships in the database
//   * @return Iterator[PathTriple]
//   */
//  override def relationships(): Iterator[PathTriple] = {
//    println("relationships()")
//
//    // iterate all relationship tables
//    val allRels = for (tableName <- relSchema.keys) yield {
//      val statement = connection.createStatement
//      val data = statement.executeQuery(s"select * from $tableName")
//
//      // transform all rows in the table to PathTriples
//      Iterator.continually(data).takeWhile(_.next())
//        .map { resultSet =>
//          val startNode = DemoNodeAt(DemoId(resultSet.getLong(":START_ID")), relMapping(tableName)._1).get
//          val endNode = DemoNodeAt(DemoId(resultSet.getLong(":END_ID")), relMapping(tableName)._2).get
//          val rel = rowToRel(resultSet, tableName, relSchema(tableName))
//
//          PathTriple(startNode, rel, endNode)
//        }
//    }
//
//    println("relationships() finished")
//    allRels.flatten.iterator
//  }
//
//  /**
//   * Get relationships with filter
//   * @param relationshipFilter the filter with specific conditions
//   * @return Iterator[PathTriple]
//   */
//  override def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] = {
//    println("relationships(relationshipFilter)")
//
//    val tableName = relationshipFilter.types.head.toString
//    val conditions = relationshipFilter.properties.map { case (key, value) => s"`${key.toString}` = '${value.value}'" }.toArray
//
//    var sql = "select * from " + tableName
//
//    // add conditions to the sql query
//    for (i <- conditions.indices) {
//      if (i == 0) {
//        sql = sql + " where " + conditions(i)
//      } else {
//        sql = sql + " and " + conditions(i)
//      }
//    }
//
//    println(sql)
//
//    val statement = connection.createStatement
//    val startTime1 = System.currentTimeMillis()
//    val data = statement.executeQuery(sql)
//    println("rel(relFilter) SQL used " + (System.currentTimeMillis() - startTime1) + " ms")
//
//    // transform the rows in the sql result to PathTriples
//    val result = Iterator.continually(data).takeWhile(_.next())
//      .map { resultSet =>
//        val startNode = DemoNodeAt(DemoId(resultSet.getLong(":START_ID")), relMapping(tableName)._1).get
//        val endNode = DemoNodeAt(DemoId(resultSet.getLong(":END_ID")), relMapping(tableName)._2).get
//        val rel = rowToRel(resultSet, tableName, relSchema(tableName))
//
//        PathTriple(startNode, rel, endNode)
//      }
//
//    result
//  }
//
//  /**
//   * Used for path like (A)-[B]->(C), where A is point to start expand, B is relationship, C is endpoint
//   * @param id id of A
//   * @param filter relationship filter of B
//   * @param direction OUTGOING (-[B]->) or INCOMING (<-[B]-)
//   * @return Iterator[PathTriple]
//   */
//  override def expand(id: LynxId, filter: RelationshipFilter, direction: SemanticDirection): Iterator[PathTriple] = {
//    println("expand()")
//
//    if (direction == BOTH) {
//      return expand(id, filter, OUTGOING) ++ expand(id, filter, INCOMING)
//    }
//
//    val tableName = filter.types.head.toString
//    val relType = filter.types.head.toString
//    val conditions = filter.properties.map { case (key, value) => s"`${key.toString}` = '${value.value}'" }.toArray
//
//    val startNode = direction match {
//      case OUTGOING => DemoNodeAt(id, relMapping(tableName)._1)
//      case INCOMING => DemoNodeAt(id, relMapping(tableName)._2)
//    }
//
//    if (startNode.isEmpty) {
//      return Iterator.empty
//    }
//
//    // start building sql query
//    var sql = s"select * from $relType"
//    direction match {
//      case OUTGOING => sql = sql + s" where $relType.`:START_ID` = ${id.toLynxInteger.value} "
//      case INCOMING => sql = sql + s" where $relType.`:END_ID` = ${id.toLynxInteger.value} "
//    }
//
//    for (i <- conditions.indices) {
//        sql = sql + " and " + conditions(i)
//    }
//
//    println(sql)
//
//    // get sql query result
//    val statement = connection.createStatement
//    val startTime1 = System.currentTimeMillis()
//    val data = statement.executeQuery(sql)
//    println("expand() SQL used: " + (System.currentTimeMillis() - startTime1) + " ms")
//
//    // transform rows in the result to PathTriples
//    val result = Iterator.continually(data).takeWhile(_.next())
//    .map { resultSet =>
//      val endNode = direction match {
//        case OUTGOING => DemoNodeAt(DemoId(resultSet.getLong(":END_ID")), relMapping(tableName)._2).get
//        case INCOMING => DemoNodeAt(DemoId(resultSet.getLong(":START_ID")), relMapping(tableName)._1).get
//      }
//
//      PathTriple(startNode.get, rowToRel(resultSet, relType, relSchema(relType)), endNode)
//    }
//
//    println("expand() totally used: " + (System.currentTimeMillis() - startTime1) + " ms")
//    result
//  }
//
//  /**
//   * Used for path like (A)-[B]->(C), where A is point to start expand, B is relationship, C is endpoint
//   * @param nodeId id of A
//   * @param filter relationship filter of B
//   * @param endNodeFilter node filter of C
//   * @param direction OUTGOING (-[B]->) or INCOMING (<-[B]-)
//   * @return Iterator[PathTriple]
//   */
//  override def expand(nodeId: LynxId, filter: RelationshipFilter,
//                      endNodeFilter: NodeFilter, direction: SemanticDirection): Iterator[PathTriple] = {
//    val result = expand(nodeId, filter, direction).filter {
//      pathTriple => endNodeFilter.matches(pathTriple.endNode)
//    }
//
//    result
//  }
//
//  /**
//   * Used for path like (A)-[B]->(C), but here A may be more than one node
//   * Allow multi-hop queries like (A)-[B*0..]->(C)
//   * @param startNodeFilter node filter of A
//   * @param relationshipFilter relationship filter of B
//   * @param endNodeFilter node filter of C
//   * @param direction OUTGOING (-[B]->) or INCOMING (<-[B]-)
//   * @param upperLimit maximum length of path
//   * @param lowerLimit minimum length of path
//   * @return Iterator[LynxPath]
//   */
//  override def paths(startNodeFilter: NodeFilter, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter,
//                     direction: SemanticDirection, upperLimit: Int, lowerLimit: Int): Iterator[LynxPath] = {
//    //TODO: 暂不支持多跳的情况
//    if (upperLimit != 1 || lowerLimit != 1) {
//      throw new RuntimeException("Upper limit or lower limit not support")
//    }
//
//    val startTime1 = System.currentTimeMillis()
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
//      var sql = s"select * from $startTable as t1 join $relType on t1.`id:ID` = "
//      direction match {
//        case OUTGOING => sql = sql + s"$relType.`:START_ID` join $endTable as t2 on t2.`id:ID` = $relType.`:END_ID`"
//        case INCOMING => sql = sql + s"$relType.`:END_ID` join $endTable as t2 on t2.`id:ID` = $relType.`:START_ID`"
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
//      println(sql)
//
//      val statement = connection.createStatement
//      val startTime1 = System.currentTimeMillis()
//      val data = statement.executeQuery(sql)
//      println("paths() combined SQL used: " + (System.currentTimeMillis() - startTime1) + " ms")
//
//      // transform rows in the result to LynxPaths
//      Iterator.continually(data).takeWhile(_.next()).map{ resultSet =>
//        PathTriple(
//          rowToNodeOffset(resultSet, startTable, nodeSchema(startTable), 0),
//          rowToRelOffset(resultSet, relType, relSchema(relType), nodeSchema(startTable).length),
//          rowToNodeOffset(resultSet, endTable, nodeSchema(endTable), nodeSchema(startTable).length + relSchema(relType).length)
//        ).toLynxPath
//      }
//    }
//
//    println("paths() combined totally used: " + (System.currentTimeMillis() - startTime1) + " ms")
//    finalResult.iterator.flatten
//  }

// 原来的paths()
/*  override def paths(startNodeFilter: NodeFilter, relationshipFilter: RelationshipFilter, endNodeFilter: NodeFilter,
            direction: SemanticDirection, upperLimit: Int, lowerLimit: Int): Iterator[LynxPath] = {
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

    val originStations = nodes(startNodeFilter)
    val result = originStations.flatMap { originStation =>
       val firstStop = expandNonStop(originStation, relationshipFilter, direction, lowerLimit)
       val leftSteps = Math.min(upperLimit, 100) - lowerLimit
       firstStop.flatMap(p => extendPath(p, relationshipFilter, direction, leftSteps))
      // expandNonStop(originStation, relationshipFilter, direction, lowerLimit)
    }.filter(_.endNode.forall(endNodeFilter.matches))

    // println("paths() finished 1")
    result
  }*/

}


