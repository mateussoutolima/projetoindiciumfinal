with produto as (

    select *
    from {{ ref('dim_produtos') }}

    ),
    regiao as (
    select *
    from {{ ref('dim_regiao') }}

    ),
    venda as (
    select *
    from {{ref('dim_venda')}}

    ),
    cliente as(
    select *
    from {{ref('dim_clientes')}}

    ),
    juntar_chaves as (
    select
    sk_produto as fk_produto
    , sk_endereco as fk_endereco
    , sk_detalhepedido as fk_detalhepedido
    , sk_cliente as fk_cliente
    , tipocartao
    , venda.razaotipo
    , status
    , quantidadepedido
    , precounitario
    , pedidovenda_id
    , subtotal

    from venda
    left join regiao on venda.territorio_id = regiao.territorio_id
    left join produto on venda.produto_id = produto.produto_id
    left join cliente on venda.cliente_id = cliente.cliente_id

    )

select * 
from juntar_chaves


