with dados_fonte as (
    select 
    row_number() over (order by stateprovinceid) as sk_estado --chave autoincremental
    , stateprovinceid	as estado_id
    , territoryid	as territorio_id
    , name as nomeestado
    , countryregioncode as pais_id
    , stateprovincecode as siglaestado	
    , modifieddate			
    , rowguid	
    , isonlystateprovinceflag						
    , _sdc_received_at			
    , _sdc_sequence					
    , _sdc_batched_at	
    , _sdc_table_version			
    	
    from {{ source('adventure_works_elt' , 'stateprovince')}}
)
    select *
    from dados_fonte