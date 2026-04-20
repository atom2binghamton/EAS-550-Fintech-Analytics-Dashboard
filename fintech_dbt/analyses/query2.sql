-- query2.sql
-- Top 10 customers by spend per product category with percentile ranking

-- 1. Aggregate total spend per customer per product category
with customer_category_spend as (

    select
        ft.customer_id,
        dc.full_name,
        dc.region,
        dp.category_name,
        sum(ft.transaction_amount)      as total_spend,
        count(ft.transaction_id)        as transaction_count,
        avg(ft.transaction_amount)      as avg_transaction_amount
    from {{ ref('fact_transactions') }} ft
    inner join {{ ref('dim_customer') }} dc
        on ft.customer_id = dc.customer_id
    inner join {{ ref('dim_product') }} dp
        on ft.product_id = dp.product_id
    where ft.transaction_status = 'Completed'
    group by
        ft.customer_id,
        dc.full_name,
        dc.region,
        dp.category_name

),

-- 2. Rank customers by spend and compute percentile within each category
ranked as (

    select
        customer_id,
        full_name,
        region,
        category_name,
        total_spend,
        transaction_count,
        avg_transaction_amount,
        rank() over (
            partition by category_name
            order by total_spend desc
        )                               as spend_rank,
        percent_rank() over (
            partition by category_name
            order by total_spend
        )                               as spend_percentile
    from customer_category_spend

)

-- 3. Return top 10 spenders per category
select
    category_name,
    spend_rank,
    full_name,
    region,
    total_spend,
    transaction_count,
    round(avg_transaction_amount, 2)    as avg_transaction_amount,
    round(spend_percentile * 100, 1)    as spend_percentile_pct
from ranked
where spend_rank <= 10
order by category_name, spend_rank
