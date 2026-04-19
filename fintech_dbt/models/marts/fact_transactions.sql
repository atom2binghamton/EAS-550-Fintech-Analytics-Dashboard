-- this is the main fact table, its the center of the star schema
-- joined accounts here to get customer_id since transactions dont have it directly

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

accounts as (
    select account_id, customer_id, account_type
    from {{ ref('stg_accounts') }}
)

select
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.product_id,
    t.transaction_date,
    -- broke out the date parts so its easier to group by month/year in queries
    date_part('year',  t.transaction_date)::int   as transaction_year,
    date_part('month', t.transaction_date)::int   as transaction_month,
    date_part('day',   t.transaction_date)::int   as transaction_day,
    date_part('dow',   t.transaction_date)::int   as transaction_day_of_week,
    date_trunc('month', t.transaction_date)::date as transaction_month_start,
    t.transaction_amount,
    t.transaction_type,
    t.transaction_channel,
    t.transaction_status,
    a.account_type
from transactions t
left join accounts a on t.account_id = a.account_id
