with dados_fonte as (
    select 
    row_number() over (order by countryregioncode) as sk_pais --chave autoincremental
    , countryregioncode as pais_id
    , name as nomepais		
    , modifieddate					
    , _sdc_sequence			
    , _sdc_table_version			
    , _sdc_received_at			
    , _sdc_batched_at	

    from {{ source('adventure_works_elt' , 'countryregion')}}
)
    select *
    from dados_fonte