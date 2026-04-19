-- subcategories staging, needed this to join later in dim_product

with source as (
    select * from {{ source('fintech_oltp', 'product_subcategories') }}
),

renamed as (
    select
        productsubcategoryid    as product_subcategory_id,
        productcategoryid       as product_category_id,
        productsubcategoryname  as product_subcategory_name
    from source
)

select * from renamed
