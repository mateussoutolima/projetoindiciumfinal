with dados_fonte as (
    select 
    row_number() over (order by addressid) as sk_endereco --chave autoincremental
    , addressid as endereco_id
    , stateprovinceid as estado_id
    , city as cidade
    , addressline1 as endereco							
    , postalcode as CEP			
    , spatiallocation as coordenadas
    , modifieddate
    , rowguid				
    , _sdc_received_at			
    , _sdc_sequence				
    , _sdc_batched_at	
    , _sdc_table_version		
    
    from {{ source('adventure_works_elt' , 'address')}}
)
    select *
    from dados_fonte