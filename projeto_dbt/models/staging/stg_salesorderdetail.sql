with dados_fonte as (
    select 
    row_number() over (order by salesorderdetailid) as sk_detalhepedido --chave autoincremental
    , salesorderdetailid as detalhepedido_id
    , salesorderid as pedidovenda_id
    , productid as produto_id
    , specialofferid as ofertaespecial_id
    , orderqty as quantidadepedido              
    , unitprice as precounitario
    , carriertrackingnumber         
    , unitpricediscount as discontounitario     
    , modifieddate          
    , rowguid           
    , _sdc_table_version                        
    , _sdc_received_at          
    , _sdc_sequence         
    , _sdc_batched_at          
        
    from {{ source('adventure_works_elt' , 'salesorderdetail')}}
)
    select *
    from dados_fonte




