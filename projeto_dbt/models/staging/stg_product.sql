with dados_fonte as (
    select 
    row_number() over (order by productid) as sk_produto --chave autoincremental
    , productid as produto_id
    , productsubcategoryid as produtosubcategoria_id
    , productmodelid as modeloproduto_id
    , name as nome
    , listprice as preco --coluna com 0
    , standardcost as custopadrao --coluna com 0
    , productnumber as numeroproduto
    , sellenddate	as datafinalvenda	
    , safetystocklevel	as nivelminimoestoque	
    , finishedgoodsflag			
    , class			
    , makeflag						
    , reorderpoint				
    , weight			
    , weightunitmeasurecode												
    , style			
    , sizeunitmeasurecode									
    , daystomanufacture			
    , productline			
    , size						
    , color
    , rowguid			
    , sellstartdate			
    , modifieddate
    , _sdc_table_version
    , _sdc_received_at			
    , _sdc_sequence
    , _sdc_batched_at

    from {{ source('adventure_works_elt' , 'product')}}
)
    select *
    from dados_fonte