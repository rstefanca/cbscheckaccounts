package cz.codingmonkey.ibs.accountchecking

import java.math.BigDecimal
import java.util

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import cz.codingmonkey.ibs.accountchecking.Entities.{Account, Client}
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.handlers.{KeyedHandler, MapListHandler, ScalarHandler}
import org.apache.commons.lang3.StringUtils

import scala.concurrent.{ExecutionContext, Future}

/**
  *
  * @author Richard Stefanca
  */
trait DB extends Queries {

  this: Config =>

  import collection.JavaConverters._

  private val dataSource = getDataSource

  def getTotalPages: Int = {
    val count = getActiveClientsCount()
    println("Active clients count: " + count)
    val p = count / pageSize
    (if ((count % pageSize) > 0) p + 1 else p).asInstanceOf[Int]
  }

  def getActiveClientsExternalIds(page: Int, pageSize: Int = pageSize)(implicit executionContext: ExecutionContext): Future[List[Client]] = {
    val query = activeExternalClientIdsPaged(page, pageSize)
    Future {
      val rows = new QueryRunner(dataSource).query(query, new MapListHandler())
      rows.asScala.map(r => Client(
        r.get("EXTERNALID").asInstanceOf[String])
      ).toList
    }
  }

  def getActiveClientsCount(): Long = {
    new QueryRunner(dataSource).query(selectCount, new ScalarHandler[java.math.BigDecimal]()).longValue()
  }

  def getClientsContractId(client: Client)(implicit executionContext: ExecutionContext): Future[Long] = {
    Future {
      new QueryRunner(dataSource).query(selectContractByClientExternalId, new ScalarHandler[java.math.BigDecimal](), client.externalClientId).longValue()
    }
  }

  def getClientsContractIds(clients: Seq[Client])(implicit executionContext: ExecutionContext): Future[List[(String, Long)]] = {
    import collection.JavaConverters._
    val ids = clients.map(_.externalClientId.asInstanceOf[Object]).toArray
    val questionMarksList = clients.map(_ => "?").asJava
    val questionMarks: String = StringUtils.join(questionMarksList, ",")
    Future {
      val results = new QueryRunner(dataSource).query(selectContractByClientExternalIds(questionMarks), new KeyedHandler("EXTERNALID"), ids: _*)
      clients.map(client => (client.externalClientId, results.get(client.externalClientId).get("CONTRACT").asInstanceOf[BigDecimal].longValue())).toList
    }
  }

  def getAccountsByContractId(contractId: Long)(implicit executionContext: ExecutionContext): Future[List[Account]] = {
    val params = Array(contractId.asInstanceOf[Object])
    Future {
      val rows = new QueryRunner(dataSource).query(selectAccountsByContract, new MapListHandler(), params:_*)
      rows.asScala.map(r => mapToAccount(r)
      ).toList
    }
  }

  private def mapToAccount(r: util.Map[String, AnyRef]) = {
    Account(
      r.get("EXTERNALID").asInstanceOf[String],
      r.get("STATUS").asInstanceOf[String],
      r.get("PRODUCTTYPE").asInstanceOf[String],
      r.get("PRODUCTSUBTYPE").asInstanceOf[String])
  }

  //WIP
  def getAccountsByContractIds(clientIdAndContractId: Seq[(String, Long)])(implicit executionContext: ExecutionContext): Future[List[(String, List[Account])]] = {
    import collection.JavaConverters._
    val contractIds = clientIdAndContractId.map(_._2.asInstanceOf[Object]).toArray
    val questionMarksList = clientIdAndContractId.map(_ => "?").asJava
    val questionMarks: String = StringUtils.join(questionMarksList, ",")
    Future {
      val rows = new QueryRunner(dataSource).query(selectAccountsByContract(questionMarks), new MapListHandler(), contractIds: _*)
      val accountsByContract = rows.asScala
        .map(r => (r.get("BASEDON").asInstanceOf[BigDecimal].longValue(), mapToAccount(r)))
        .groupBy(_._1)

      clientIdAndContractId.map(ci => (ci._1, accountsByContract.getOrElse(ci._2, List.empty).map(_._2).toList)).toList
    }
  }

  def closeDataSource(): Unit = {
    dataSource.close()
  }

  private def getDataSource: HikariDataSource = {

    println(s"dbUrl: $databaseUrl")
    println(s"username: $databaseUser")

    val config = new HikariConfig()
    config.setDriverClassName("oracle.jdbc.OracleDriver")
    config.setJdbcUrl(databaseUrl)
    config.setUsername(databaseUser)
    config.setPassword(databasePassword)
    config.setConnectionTestQuery("SELECT 1 FROM DUAL")

    //pool config
    config.setMaximumPoolSize(8)
    config.setConnectionTimeout(1000)

    val ds = new HikariDataSource(config)
    ds
  }

}


