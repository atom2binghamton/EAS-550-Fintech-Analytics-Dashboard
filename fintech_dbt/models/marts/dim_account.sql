-- account dimension, joined with customer so we dont have to join every time in queries

with accounts as (
    select * from {{ ref('stg_accounts') }}
),

customers as (
    select customer_id, full_name, region, customer_status
    from {{ ref('stg_customers') }}
)

select
    a.account_id,
    a.customer_id,
    c.full_name        as customer_name,
    c.region           as customer_region,
    a.account_type,
    a.open_date,
    a.closed_date,
    a.account_status,
    a.registration_id,
    a.balance,
    -- added this to easily filter open vs closed accounts
    case
        when a.closed_date is not null then 'Closed'
        else 'Open'
    end as is_active
from accounts a
left join customers c on a.customer_id = c.customer_id
