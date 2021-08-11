with dados_fonte as (
    select 
    row_number() over (order by productid) as sk_product --chave autoincremental checar com stg_product
    , productid as produto_id
    , listprice as preco
    , startdate			
    , modifieddate								
    , enddate			
    , _sdc_sequence			
    , _sdc_table_version			
    , _sdc_received_at			
    , _sdc_batched_at		
    from {{ source('adventure_works_elt' , 'productlistpricehistory')}}
)
    select *
    from dados_fonte