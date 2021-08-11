with dados_fonte as (
    select 
    row_number() over (order by productcategoryid) as sk_categoriaproduto --chave autoincremental
	, productcategoryid as categoriaproduto_id
    , name as nome
    , rowguid				
    , _sdc_sequence			
    , _sdc_table_version			

    from {{ source('adventure_works_elt' , 'productcategory')}}
)
    select *
    from dados_fonte