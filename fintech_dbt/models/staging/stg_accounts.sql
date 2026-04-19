-- same thing as customers but for accounts table

with source as (
    select * from {{ source('fintech_oltp', 'accounts') }}
),

renamed as (
    select
        accountid       as account_id,
        customerid      as customer_id,
        accounttype     as account_type,
        opendate        as open_date,
        closeddate      as closed_date,
        status          as account_status,
        registrationid  as registration_id,
        balance
    from source
)

select * from renamed
