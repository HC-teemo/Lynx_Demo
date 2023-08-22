package org.example.demo2


import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.types.structural.{LynxPropertyKey,LynxNodeLabel,LynxRelationshipType}
import org.grapheco.lynx.types.time.LynxDate

import java.sql.ResultSet
import java.time.LocalDate

object Mapper {
  private final val ID_COL_NAME: String = "id:ID"
  private final val REL_ID_COL_NAME: String = "REL_ID"
  private final val START_ID_COL_NAME: String = ":START_ID"
  private final val END_ID_COL_NAME: String = ":END_ID"

  private def mapId(row: ResultSet): DemoId = DemoId(row.getLong(ID_COL_NAME))
  private def mapRelId(row: ResultSet): DemoId = DemoId(row.getLong(REL_ID_COL_NAME))
  private def mapStartId(row: ResultSet): DemoId = DemoId(row.getLong(START_ID_COL_NAME))
  private def mapEndId(row: ResultSet): DemoId = DemoId(row.getLong(END_ID_COL_NAME))

  private def mapProps(row: ResultSet, mapper: Array[(String,String)]): Map[LynxPropertyKey, LynxValue] = {
    mapper.map{ case (col, typo) =>
      val i = row.findColumn(col)
      LynxPropertyKey(col) -> LynxValue(typo match {
        case "String" => row.getString(i)
        case "BIGINT" => row.getLong(i)
        case "INT" => row.getInt(i)
        case "Date" => LocalDate.ofEpochDay(row.getDate(i).getTime / 86400000)
        case _ => row.getString(i)
      })
    }.toMap
  }

  def mapNode(row: ResultSet, tableName: String, mapper: Array[(String, String)]): DemoNode =
    DemoNode(mapId(row), Seq(LynxNodeLabel(tableName)), mapProps(row, mapper))

  def mapRel(row: ResultSet, relName: String, mapper: Array[(String, String)]): DemoRelationship =
    DemoRelationship(mapRelId(row), mapStartId(row), mapEndId(row), Some(LynxRelationshipType(relName)), mapProps(row, mapper))

}
