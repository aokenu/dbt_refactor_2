with 

source as (

    select * from {{ source('stripe', 'stripe_payments') }}

),

transformed as (
    
    select 

        id as payment_id,
        orderid as order_id,
        created as order_placed_at,
        max(created) as payment_finalized_date, 
        sum(amount) / 100.0 as total_amount_paid,
        status as order_status

    from source

        where status <> 'fail'
        group by 1,2,3,6
        
)

select * from transformed