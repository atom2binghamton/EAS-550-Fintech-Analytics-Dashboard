-- product dimension
-- the original schema had 3 tables for products so i flatten them all into one here
-- easier to use when querying

with products as (
    select * from {{ ref('stg_products') }}
),

subcategories as (
    select * from {{ ref('stg_product_subcategories') }}
),

categories as (
    select * from {{ ref('stg_product_categories') }}
)

select
    p.product_id,
    p.product_name,
    s.product_subcategory_id,
    s.product_subcategory_name,
    c.product_category_id,
    c.product_category_name
from products p
left join subcategories s on p.product_subcategory_id = s.product_subcategory_id
left join categories c on s.product_category_id = c.product_category_id
