package cz.codingmonkey.ibs.accountchecking

/**
  *
  * @author Richard Stefanca
  */
trait Queries {

  //na startu nacist mapy GFO_PRODUCTSTATUS(CODE, EXTERNALVALUE) a GFO_PRODUCTSUBTYPE(CODE, EXTERNALVALUE) pro pouziti pri vypoctu chechsumu
  // pouzivaji se external values

  def activeExternalClientIdsPaged(page: Int, pageSize: Int = 100): String = {
    s"""SELECT * FROM
       |(
       |    SELECT a.*, rownum r__
       |    FROM
       |    (
       |        select distinct client.externalid
       |		    from GFO_PARTYCONTRACT pc
       |		    right join GFO_CONTRACT conBasedOn on pc.BASEDON=conBasedOn.ID
       |		    join  GFO_PARTYCONTRACT pcBasedOn on pcBasedOn.contract = conBasedOn.ID
       |		    join GFO_CLIENT client on pcBasedOn.BASEDON=client.contract
       |		    left join GFO_CONTRACT con on pc.CONTRACT=con.ID
       |		    where (con.VALIDTO is null or con.VALIDTO > SYSDATE)
       |		    and pcBasedOn.ENTITYSTATUS = 'A'
       |        order by client.externalid
       |
      |    ) a
       |    WHERE rownum < (($page * $pageSize) + 1 )
       |)
       |WHERE r__ >= ((($page-1) * $pageSize) + 1)""".stripMargin
  }

  val selectCount: String =
    """
      |select count(distinct client.externalid)
      |		    from GFO_PARTYCONTRACT pc
      |		    right join GFO_CONTRACT conBasedOn on pc.BASEDON=conBasedOn.ID
      |		    join  GFO_PARTYCONTRACT pcBasedOn on pcBasedOn.contract = conBasedOn.ID
      |		    join GFO_CLIENT client on pcBasedOn.BASEDON=client.contract
      |		    left join GFO_CONTRACT con on pc.CONTRACT=con.ID
      |		    where (con.VALIDTO is null or con.VALIDTO > SYSDATE)
      |		    and pcBasedOn.ENTITYSTATUS = 'A'
    """.stripMargin

  val selectContractByClientExternalId: String =
    """
      |select partyContract.contract from GFO_PARTYCONTRACT partyContract, GFO_CLIENT client
      |where client.contract=partyContract.contract and client.externalId=? and partyContract.partyRole='O'
    """.stripMargin

  val selectAccountsByContract: String =
    """
      |select EXTERNALID, GFO_PRODUCT.PRODUCTTYPE, GFO_PRODUCT.PRODUCTSUBTYPE, GFO_PRODUCT.STATUS from GFO_PRODUCT  join GFO_PARTYCONTRACT  USING (contract) where basedon=? and producttype = 'A' and STATUS <> 'CLOSED' and status <> 'CLOSED_CANCEL'

    """.stripMargin

}
