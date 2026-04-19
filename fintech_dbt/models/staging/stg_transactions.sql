-- staging for transactions, just renaming the columns here

with source as (
    select * from {{ source('fintech_oltp', 'transactions') }}
),

renamed as (
    select
        transactionid       as transaction_id,
        accountid           as account_id,
        productid           as product_id,
        transactiondate     as transaction_date,
        transactionamount   as transaction_amount,
        transactiontype     as transaction_type,
        transactionchannel  as transaction_channel,
        status              as transaction_status
    from source
)

select * from renamed
