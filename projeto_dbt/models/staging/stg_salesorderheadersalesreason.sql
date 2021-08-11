with dados_fonte as (
    select 
    row_number() over (order by salesreasonid) as sk_motivovenda --chave autoincremental
    , salesreasonid as motivovenda_id
    , salesorderid as pedidovenda_id	
    , modifieddate						
    , _sdc_sequence			
    , _sdc_table_version			
    , _sdc_received_at			
    , _sdc_batched_at	
    	
    from {{ source('adventure_works_elt' , 'salesorderheadersalesreason')}}
)
    select *
    from dados_fonte