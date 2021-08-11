with staging as (

    select
    sk_cliente --chave autoincremental
    , cliente_id
    , titulo
    , nome
    , sobrenome	
    , estilonome	
    , tipocliente	
    
    from {{ref('stg_person')}}

)

select * from staging
