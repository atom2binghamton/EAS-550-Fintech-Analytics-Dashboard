-- query3.sql
-- Customer cohort retention analysis by join month

-- 1. Assign each customer to their join-month cohort
with customer_cohorts as (

    select
        dc.customer_id,
        date_trunc('month', dc.join_date)::date         as cohort_month
    from {{ ref('dim_customer') }} dc

),

-- 2. Collapse transactions to one row per customer per active month
customer_activity as (

    select
        ft.customer_id,
        ft.transaction_month_start                      as activity_month
    from {{ ref('fact_transactions') }} ft
    where ft.transaction_status = 'Completed'
    group by ft.customer_id, ft.transaction_month_start

),

-- 3. Join cohort assignment onto activity and compute months since join
cohort_activity as (

    select
        cc.cohort_month,
        ca.activity_month,
        ca.customer_id,
        (
            (date_part('year',  ca.activity_month) - date_part('year',  cc.cohort_month)) * 12
            + date_part('month', ca.activity_month) - date_part('month', cc.cohort_month)
        )::int                                          as months_since_join
    from customer_cohorts cc
    inner join customer_activity ca
        on cc.customer_id = ca.customer_id

),

-- 4. Count total customers per cohort for the retention rate denominator
cohort_sizes as (

    select
        cohort_month,
        count(distinct customer_id)                     as cohort_size
    from customer_cohorts
    group by cohort_month

),

-- 5. Count active customers per cohort per month
monthly_retention as (

    select
        cohort_month,
        activity_month,
        months_since_join,
        count(distinct customer_id)                     as active_customers
    from cohort_activity
    group by cohort_month, activity_month, months_since_join

)

-- 6. Join sizes onto activity counts and calculate retention rate
select
    mr.cohort_month,
    cs.cohort_size,
    mr.activity_month,
    mr.months_since_join,
    mr.active_customers,
    round(
        mr.active_customers::numeric / cs.cohort_size * 100,
        1
    )                                                   as retention_rate_pct
from monthly_retention mr
inner join cohort_sizes cs
    on mr.cohort_month = cs.cohort_month
order by mr.cohort_month, mr.months_since_join
