with dados_fonte as (
    select 
    row_number() over (order by salesreasonid) as sk_motivovenda --chave autoincremental checar com stg_salesorderheadersalesreason
    , salesreasonid as motivovenda_id
    , name as nome
    , reasontype as razaotipo		
    , modifieddate									
    , _sdc_sequence			
    , _sdc_table_version			
    , _sdc_received_at			
    , _sdc_batched_at

    from {{ source('adventure_works_elt' , 'salesreason')}}
)
    select *
    from dados_fonte