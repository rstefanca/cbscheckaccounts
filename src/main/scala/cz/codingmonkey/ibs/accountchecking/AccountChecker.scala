package cz.codingmonkey.ibs.accountchecking

import java.io.File

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.stream.scaladsl._
import akka.stream.{ActorAttributes, ActorMaterializer, FlowShape}
import akka.util.ByteString
import cz.codingmonkey.ibs.accountchecking.Entities._
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.StringUtils.isNumeric

import scala.concurrent.Future

/**
  *
  * @author Richard Stefanca
  */
object AccountChecker extends Config with DB {


  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("accountchecker")
    import system.dispatcher

    val log: LoggingAdapter = Logging(system, getClass)

    implicit val materializer = ActorMaterializer()

    println("Starting")

    val ioDispatcherAttributes = ActorAttributes.dispatcher("io-dispatcher")

    lazy val dbDispatcher = system.dispatchers.lookup("io-dispatcher") // todo special db dispatcher

    lazy val accountCheckResultFile: File = new File("account_check.csv")

    val resultSink: Sink[ByteString, Future[Long]] = FileIO.toFile(accountCheckResultFile, append = false).withAttributes(ioDispatcherAttributes)

    def loadIbsAccountsAndCalculateChecksums(batch: Seq[Client]): Future[List[IbsClientIdAndChecksum]] = {
      //inner stream to calculate account checksum for each client
      Source(batch.toList)
        .mapAsync(2) { client =>
          getClientsContractId(client)(dbDispatcher).map(contract => (client.externalClientId, contract))
        }
        .mapAsync(2) { clientIdAndContract =>
          getAccountsByContractId(clientIdAndContract._2)(dbDispatcher).map(accounts => (clientIdAndContract._1, accounts))
        }
        .map(clientAndAccounts => calculateIbsAccountsChecksum(clientAndAccounts))
        .runFold(List.empty[IbsClientIdAndChecksum]) { (list, checksum) => list :+ checksum }
    }

    // ---- FLOWS ----

    /**
      * Cleans non-numeric values from the batch. Keeps the size of the batch - batches are converted into stream of single items
      * then filtered and than again grouped by given page size
      *
      * @return flow of batches of ids
      */
    def clean: Flow[List[Client], Seq[Client], Unit] = Flow[List[Client]]
      .mapConcat(identity)
      .filter(client => isNumeric(client.externalClientId))
      .grouped(pageSize)

    def wsFlow: Flow[Seq[Client], CbsClientIdAndChecksum, Unit] = Flow[Seq[Client]]
      // TODO implement ws invocation
      //.map(WS.preparePayload)
      //.mapAsync(1) { body => WS.cbsAccountChecksums(body) }
      //.map(resp => parseResponse(resp))
      .mapAsync(2) { d => {
      Future {
        //Thread.sleep(500) //simulate ws call
        d
      }(dbDispatcher)
    }
    }
      .map(l => l.map(client => CbsClientIdAndChecksum(client.externalClientId, Some("fadfa9303840934"))).toList)
      .mapConcat(identity)

    /**
      * Gets accounts  and computes checksums for external ids
      *
      * @return source of IbsClientIdAndChecksum
      */
    def dbFlow: Flow[Seq[Client], IbsClientIdAndChecksum, Unit] = Flow[Seq[Client]]
      .mapAsync(4) { l => loadIbsAccountsAndCalculateChecksums(l) }
      .mapConcat(identity)

    /**
      * Flow for parallel processing. Both parallel streams are combined into [[AccountCheckSumsSummary]]
      */
    val checkSumsFlow =
      Flow.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        // prepare graph elements
        val broadcast = b.add(Broadcast[Seq[Client]](2))
        val zip = b.add(ZipWith[CbsClientIdAndChecksum, IbsClientIdAndChecksum, AccountCheckSumsSummary]((cbs, ibs) => combineToSummary(cbs, ibs)))

        // connect the graph
        broadcast.out(0) ~> wsFlow ~> zip.in0
        broadcast.out(1) ~> dbFlow ~> zip.in1

        // expose ports
        FlowShape(broadcast.in, zip.out)
      })

    //----- MAIN PROCESS -----
    /* IBS - imaginary banking system :)
    *  STAGES:
    *   1. takes batches of ids from IBS db by given page size
    *   2. cleans non-numeric values
    *   3. for each batch computes & gets checksums in parallel
    *     - retrieves checksums from CBS
    *     - retrieves accounts and computes checksums from IBS db
    *     - combines results together into stream of single results by external client id
    *   4. outputs results to file
    *
    * */

    val start = System.currentTimeMillis()
    Source(1 to getTotalPages)
      .mapAsync(1) { page => getActiveClientsExternalIds(page)(dbDispatcher) }
      .via(clean)
      .via(checkSumsFlow)
      .map(line => ByteString(s"${q(line.clientIdCbs)},${q(line.cbsCheckSum)},${q(line.ibsCheckSum)},${q(line.input)},${q(line.matching)}" + System.lineSeparator()))
      .runWith(resultSink)
      .onComplete(_ => {
        val stop = System.currentTimeMillis()
        println(s"Duration: ${stop - start}")
        println("Done :)")
        closeDataSource()
        system.shutdown()
      })
  }

  //-----------------------------------

  private def calculateIbsAccountsChecksum(clientIdAndAccounts: (String, List[Account])) = {
    val sb = new StringBuilder
    clientIdAndAccounts._2.foreach(account => {
      // No record separator
      sb.append(account.externalId)
      sb.append(";")
      sb.append(account.productType)
      sb.append(";")
      sb.append(account.productSubtype)
      sb.append(";")
      sb.append(account.status)
    })
    val input = if (sb.isEmpty) None else Some(sb.toString())
    IbsClientIdAndChecksum(clientIdAndAccounts._1, input.map(DigestUtils.md5Hex), input)
  }

  private def combineToSummary(cbs: CbsClientIdAndChecksum, ibs: IbsClientIdAndChecksum) = {
    if (cbs.clientId != ibs.clientId) throw new IllegalStateException(s"Unexpected Ids mismatch ${cbs.clientId} - ${ibs.clientId}")
    AccountCheckSumsSummary(
      cbs.clientId,
      ibs.clientId,
      cbs.checksum.getOrElse("NA"),
      ibs.checksum.getOrElse("NA"),
      ibs.input.getOrElse("NA"),
      printMatchResult(cbs, ibs))
  }

  private def printMatchResult(cbs: CbsClientIdAndChecksum, ibs: IbsClientIdAndChecksum) = {
    if (ibs.checksum.isDefined && cbs.checksum.isDefined) {
      if (ibs.checksum == cbs.checksum)
        "MATCH"
      else
        "MISMATCH"
    } else {
      if (ibs.checksum.isDefined && cbs.checksum.isEmpty) {
        "MISSING CBS"
      } else if (ibs.checksum.isEmpty && cbs.checksum.isDefined) {
        "MISSING IBS"
      } else "MISSING BOTH"
    }
  }

  private def q(str: String): String = "\"" + str + "\""

}
