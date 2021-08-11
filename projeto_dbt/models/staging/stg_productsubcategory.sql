with dados_fonte as (
    select
    row_number() over (order by productsubcategoryid) as sk_subcategoriaproduto --chave autoincremental
    , productsubcategoryid as produtosubcategoria_id
    , productcategoryid as categoriaproduto_id
    , name as nome
    , modifieddate			
    , rowguid						
    , _sdc_sequence			
    , _sdc_table_version			
    , _sdc_received_at			
    , _sdc_batched_at	
    	
    from {{ source('adventure_works_elt' , 'productsubcategory')}}
)
    select *
    from dados_fonte