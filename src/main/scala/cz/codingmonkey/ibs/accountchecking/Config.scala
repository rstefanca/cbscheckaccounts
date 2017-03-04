package cz.codingmonkey.ibs.accountchecking

import com.typesafe.config.ConfigFactory

/**
  *
  * @author rstefanca
  */
trait Config {

  private val config = ConfigFactory.load()
  private val databaseConfig = config.getConfig("db")

  val databaseUrl: String = databaseConfig.getString("url")
  val databaseUser: String = databaseConfig.getString("user")
  val databasePassword:String = databaseConfig.getString(" password")

  val pageSize: Int = config.getInt("checker.pageSize")


}
