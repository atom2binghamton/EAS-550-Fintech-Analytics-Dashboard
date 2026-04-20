-- query1.sql
-- Monthly regional revenue with running total and month-over-month growth rate

-- 1. Aggregate completed transactions by region and month
with monthly_region_revenue as (

    select
        dc.region,
        ft.transaction_month_start,
        sum(ft.transaction_amount)  as monthly_revenue,
        count(ft.transaction_id)    as transaction_count
    from {{ ref('fact_transactions') }} ft
    inner join {{ ref('dim_customer') }} dc
        on ft.customer_id = dc.customer_id
    where ft.transaction_status = 'Completed'
    group by dc.region, ft.transaction_month_start

),

-- 2. Apply window functions for running total and previous month revenue
with_window_calcs as (

    select
        region,
        transaction_month_start,
        monthly_revenue,
        transaction_count,
        sum(monthly_revenue) over (
            partition by region
            order by transaction_month_start
            rows between unbounded preceding and current row
        )                                                           as running_total,
        lag(monthly_revenue) over (
            partition by region
            order by transaction_month_start
        )                                                           as prev_month_revenue
    from monthly_region_revenue

)

-- 3. Calculate month-over-month growth rate and return final result
select
    region,
    transaction_month_start,
    monthly_revenue,
    transaction_count,
    running_total,
    prev_month_revenue,
    round(
        (monthly_revenue - prev_month_revenue)
        / nullif(prev_month_revenue, 0) * 100,
        2
    )                                                               as mom_growth_pct
from with_window_calcs
order by region, transaction_month_start
