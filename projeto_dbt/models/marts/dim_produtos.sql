with produto as (
    select

    sk_produto
    , produto_id
    , produtosubcategoria_id
    , modeloproduto_id
    , nome
    , preco --valor 0
    , custopadrao -- valor 0
    , numeroproduto
    , datafinalvenda
    , nivelminimoestoque
    
    from {{ref('stg_product')}}
    ),
    categoriaproduto as (
    select

    sk_categoriaproduto 
	, categoriaproduto_id
    , nome
    
    from {{ref('stg_productcategory')}}
    ),
    subcategoriaproduto as (
    select

    sk_subcategoriaproduto 
    , produtosubcategoria_id
    , categoriaproduto_id
    , nome
    
    from {{ref('stg_productsubcategory')}}
    ),
    historicopreco as (
    select

    sk_product --chave autoincremental checar com stg_product
    , produto_id
    , preco
    
    from {{ref('stg_productlistpricehistory')}}
    ),

    dados_agregados as (
    select

    produto.sk_produto
    , produto.produto_id
    , produto.produtosubcategoria_id
    , modeloproduto_id
    , categoriaproduto.categoriaproduto_id
    , produto.nome
    , historicopreco.preco
    , custopadrao -- valor 0
    , numeroproduto
    , datafinalvenda
    , nivelminimoestoque

    from produto
    left join subcategoriaproduto on produto.produtosubcategoria_id = subcategoriaproduto.produtosubcategoria_id
    left join historicopreco on produto.produto_id = historicopreco.produto_id
    left join categoriaproduto on subcategoriaproduto.categoriaproduto_id = categoriaproduto.categoriaproduto_id

    )

    select *
    from dados_agregados