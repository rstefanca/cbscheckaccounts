package cz.codingmonkey.ibs.accountchecking

import cz.codingmonkey.ibs.accountchecking.Entities.CbsClientIdAndChecksum

import scala.concurrent.{ExecutionContext, Future}
import scala.xml.Elem

/**
  *
  * @author Richard Stefanca
  */
//TODO - implement WS invocation using akka-http
object WS {

  def preparePayload(externalClientIds: List[String]): String = {
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
      <soapenv:Header/>
      <soapenv:Body>
          <CheckAccountCheckSumsRequest>
        {externalClientIds.map(externalId => {
        <ClientId>{externalId}</ClientId>
      }
      )}
          </CheckAccountCheckSumsRequest>
      </soapenv:Body>
    </soapenv:Envelope>.toString()
  }

  def cbsAccountChecksums(payload: String)(implicit executionContext: ExecutionContext) = Future {
    Thread.sleep(300) //simulate io
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Header/>
    <soapenv:Body>
    <CheckAccountCheckSumsResponse>
      <Results>
        <Result>
        <ClientId>65401</ClientId>
        <Checksum>fadfa9303840934</Checksum>
        </Result>
        <Result>
          <ClientId>65402</ClientId>
          <Checksum>fadfa9303840934</Checksum>
        </Result>
        <Result>
          <ClientId>65403</ClientId>
          <Checksum>fadfa9303840934</Checksum>
        </Result>
        <Result>
          <ClientId>65404</ClientId>
          <Checksum>fadfa9303840934</Checksum>
        </Result>
        <Result>
          <ClientId>65441</ClientId>
          <Checksum>fadfa9303840934</Checksum>
        </Result>
        <Result>
          <ClientId>654654</ClientId>

        </Result>
      </Results>
    </CheckAccountCheckSumsResponse>
    </soapenv:Body>
    </soapenv:Envelope>
  }

  def parseResponse(resp: Elem): List[CbsClientIdAndChecksum] = {

    println("ws result")
    val resultsXml = resp \ "Body" \ "CheckAccountCheckSumsResponse" \ "Results" \ "Result"
    resultsXml.map(result => {
      val checksum = (result \ "Checksum").text
      val maybeChecksum = if (checksum.isEmpty) None else Some(checksum)
      CbsClientIdAndChecksum((result \ "ClientId").text, maybeChecksum)
    }).toList
  }



}
