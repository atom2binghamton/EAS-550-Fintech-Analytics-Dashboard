-- products staging

with source as (
    select * from {{ source('fintech_oltp', 'products') }}
),

renamed as (
    select
        productid               as product_id,
        productsubcategoryid    as product_subcategory_id,
        productname             as product_name
    from source
)

select * from renamed
