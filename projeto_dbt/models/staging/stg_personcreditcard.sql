with dados_fonte as (
    select 
    row_number() over (order by creditcardid) as sk_cartao --chave autoincremental			
    , creditcardid	as 	cartaocredito_id
    , businessentityid as cliente_id
    , modifieddate			
    , _sdc_sequence			
    , _sdc_table_version			
    , _sdc_received_at			
    , _sdc_batched_at			
				

    from {{ source('adventure_works_elt' , 'personcreditcard')}}
)
    select *
    from dados_fonte

		