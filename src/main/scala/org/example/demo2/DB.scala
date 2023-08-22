package org.example.demo2

import com.typesafe.scalalogging.LazyLogging

import java.sql.{Connection, DriverManager, ResultSet}

object DB extends LazyLogging{
  val url = "jdbc:mysql://10.0.82.144:3306/tpch?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false"
  val driver = "com.mysql.cj.jdbc.Driver"
  val username = "root"
  val password = "Hc1478963!"
  Class.forName(driver)

  val connection: Connection = DriverManager.getConnection(url, username, password)

  def sql(sql: String): Unit = {
    logger.info(sql)
    val statement = connection.createStatement()
    statement.executeQuery(sql)
  }

  def iterExecute(sql: String): Iterator[ResultSet] = {
    logger.info(sql)
    val statement = connection.createStatement()
    Iterator.continually(statement.executeQuery(sql)).takeWhile(_.next())
  }
}