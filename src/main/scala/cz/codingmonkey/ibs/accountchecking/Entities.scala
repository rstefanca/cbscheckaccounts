package cz.codingmonkey.ibs.accountchecking

/**
  *
  * @author Richard Stefanca
  */
object Entities {

  case class Client(externalClientId: String)

  case class CbsClientIdAndChecksum(clientId: String, checksum: Option[String])

  case class IbsClientIdAndChecksum(clientId: String, checksum: Option[String], input: Option[String])

  case class AccountCheckSumsSummary(clientIdCbs: String, clientIdIbs: String, cbsCheckSum: String, ibsCheckSum: String, input: String, matching: String)

  case class Account(externalId: String, status: String, productSubtype: String)

}
