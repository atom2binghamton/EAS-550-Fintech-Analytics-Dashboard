-- top level categories

with source as (
    select * from {{ source('fintech_oltp', 'product_categories') }}
),

renamed as (
    select
        productcategoryid       as product_category_id,
        productcategoryname     as product_category_name
    from source
)

select * from renamed
