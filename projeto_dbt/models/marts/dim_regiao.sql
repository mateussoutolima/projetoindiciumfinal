
with endereco as (
    select
    sk_endereco 
    , endereco_id
    , estado_id
    , cidade						
    , CEP			
    , coordenadas
    
    from {{ref('stg_address')}}
    ),

    estado as (
    select
    sk_estado 
    , estado_id
    , territorio_id
    , nomeestado
    , pais_id
    , siglaestado
    
    from {{ref('stg_stateprovince')}}
    ),

    pais as (
    select
    sk_pais
    , pais_id
    , nomepais		
    
    from {{ref('stg_countryregion')}}
    
    ),

    dados_agregados as (
    select
    sk_endereco 
    , endereco_id
    , estado.estado_id
    , pais.pais_id
    , territorio_id
    , cidade
	, nomeestado	
    , nomepais
    , CEP
    , siglaestado				
    , coordenadas

        from endereco
        left join estado on endereco.estado_id = estado.estado_id
        left join pais on estado.pais_id = pais.pais_id
	
    )

select * from dados_agregados


