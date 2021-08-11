with dados_fonte as (
    select 
    row_number() over (order by businessentityid) as sk_cliente --chave autoincremental
    , businessentityid as cliente_id
    , title as titulo
    , firstname as nome
    , lastname	as sobrenome	
    , namestyle	as estilonome
    , persontype as tipocliente	
    , middlename -- Coluna vazia		
    , suffix -- Coluna vazia					
    , emailpromotion
    , modifieddate			
    , rowguid	
	, _sdc_table_version		
    , _sdc_received_at			
    , _sdc_sequence						
    , _sdc_batched_at			 			

    from {{ source('adventure_works_elt' , 'person')}}
)
    select *
    from dados_fonte