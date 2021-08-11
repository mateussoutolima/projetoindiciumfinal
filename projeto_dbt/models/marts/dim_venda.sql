

  with dados_pedidovenda as (
    select
    sk_pedidovenda
    , pedidovenda_id
    , meioenvido_id
    , vendedor_id
    , cambio_id	
    , territorio_id
    , cliente_id
    , enderecoenvio_id
    , enderecocobranca_id
    , cartaocredito_id
    , status
    , subtotal
    , totaldevido	
    , numeroordemcompram
    
    from {{ref('stg_salesorderheader')}}
    ),


    detalhespedido as (
    select
    sk_detalhepedido 
    , detalhepedido_id
    , pedidovenda_id
    , produto_id
    , ofertaespecial_id
    , quantidadepedido              
    , precounitario         
    , discontounitario     
    
    from {{ref('stg_salesorderdetail')}}
    
    ),

    cartaodecredito as (
    select
    sk_cartao --chave autoincremental
  	, cartaocredito_id
    , tipocartao			  
    
    from {{ref('stg_creditcard')}}
    
    ),

    cartaocliente as (
    select
    sk_cartao --chave autoincremental			
    , cartaocredito_id
    , cliente_id	  
    
    from {{ref('stg_personcreditcard')}}
    
    ),

    motivovenda as (
    select
    sk_motivovenda
    , motivovenda_id
    , nome
    , razaotipo		
    
    from {{ref('stg_salesreason')}}
    
    ),

    pedidomotivovenda as (
    select
    sk_motivovenda
    , motivovenda_id
    , pedidovenda_id		
    
    from {{ref('stg_salesorderheadersalesreason')}}
    
    
    ),

    dados_agregados as (
    select
   sk_pedidovenda
    , sk_detalhepedido
    , cartaodecredito.sk_cartao
    , detalhespedido.pedidovenda_id
  	, cartaodecredito.cartaocredito_id
    , cartaocliente.cliente_id
    , tipocartao
    , razaotipo 
    , meioenvido_id
    , vendedor_id
    , cambio_id	
    , territorio_id
    , enderecoenvio_id
    , enderecocobranca_id
    , ofertaespecial_id
    , detalhepedido_id
    , produto_id
    , status
    , subtotal
    , totaldevido	
    , numeroordemcompram
    , quantidadepedido              
    , precounitario         
    , discontounitario     

        from detalhespedido
        left join dados_pedidovenda on detalhespedido.pedidovenda_id = dados_pedidovenda.pedidovenda_id
        left join cartaodecredito on dados_pedidovenda.cartaocredito_id = cartaodecredito.cartaocredito_id
        left join cartaocliente on cartaodecredito.cartaocredito_id = cartaocliente.cartaocredito_id
        left join pedidomotivovenda on dados_pedidovenda.pedidovenda_id = pedidomotivovenda.pedidovenda_id
        left join motivovenda on pedidomotivovenda.motivovenda_id = motivovenda.motivovenda_id

    )

select * from dados_agregados


