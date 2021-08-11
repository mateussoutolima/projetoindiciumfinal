with dados_fonte as (
    select 
    row_number() over (order by salesorderid) as sk_pedidovenda --chave autoincremental
    , salesorderid as pedidovenda_id			
    , shipmethodid as meioenvido_id
    , shiptoaddressid as enderecoenvio_id			
    , billtoaddressid as enderecocobranca_id		
    , salespersonid as vendedor_id
    , currencyrateid as cambio_id	
    , territoryid as territorio_id	
    , creditcardid as cartaocredito_id
    , customerid as cliente_id
    , status
    , subtotal
    , totaldue as totaldevido	
    , purchaseordernumber as numeroordemcompram
    , modifieddate			
    , rowguid			
    , taxamt					
    , onlineorderflag			
    , accountnumber				
    , orderdate						
    , creditcardapprovalcode						
    , revisionnumber			
    , freight			
    , duedate									
    , shipdate
    , _sdc_table_version
    , _sdc_received_at			
    , _sdc_sequence			
    , _sdc_batched_at			
    	
    from {{ source('adventure_works_elt' , 'salesorderheader')}}
)
    select *
    from dados_fonte