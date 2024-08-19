with 

customers as(

    select * from {{ ref("stg_jaffle_shop_customers") }}

),

orders as (

    select * from {{ ref("int_orders") }}
    
),




final as (

    select
        paid_orders.customer_id,
        paid_orders.givenname,
        paid_orders.surname,
        payments.*,
        row_number() over (order by paid_orders.paid_order_id) as transaction_seq,
        row_number() over (partition by paid_orders.customer_id order by paid_orders.paid_order_id) as customer_sales_seq,
        case when paid_orders.paid_first_order_date = paid_orders.paid_order_placed_at 
            then 'new'
            else 'return' 
        end as nvsr,
        paid_orders.clv_bad as customer_lifetime_value,
        paid_orders.paid_first_order_date as fdos
    from {{ ref("stg_stripe_payments") }} as payments

    left outer join {{ ref("int_orders") }} as paid_orders

    on paid_orders.order_id = payments.order_id

    order by payments.order_id
    )



select * from final