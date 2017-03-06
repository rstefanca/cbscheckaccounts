package cz.codingmonkey.ibs.accountchecking

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import cz.codingmonkey.ibs.accountchecking.Entities.{Account, Client}
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.handlers.{MapListHandler, ScalarHandler}

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

  def getAccountsByContractId(contractId: Long)(implicit executionContext: ExecutionContext): Future[List[Account]] = {
    val params = Array(contractId.asInstanceOf[Object])
    Future {
      val rows = new QueryRunner(dataSource).query(selectAccountsByContract, new MapListHandler(), params:_*)
      rows.asScala.map(r => Account(
        r.get("EXTERNALID").asInstanceOf[String],
        r.get("STATUS").asInstanceOf[String],
        r.get("PRODUCTTYPE").asInstanceOf[String],
        r.get("PRODUCTSUBTYPE").asInstanceOf[String])
      ).toList
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


