with 

orders as (

    select * from {{ ref("stg_jaffle_shop_orders") }}
    
),



payments as (

    select * from {{ ref("stg_stripe_payments") }}


),



paid_orders as (        
    
    select 
        orders.customer_id as customer_id,
        orders.order_id as order_id,
        orders.order_placed_at as order_placed_at,
        orders.first_order_date as paid_first_order_date,
        payments.order_id as paid_order_id,
        payments.order_status as paid_order_status,
        payments.order_placed_at as paid_order_placed_at,
        payments.payment_finalized_date as paid_payment_finalized_date,
        sum(payments.total_amount_paid) as clv_bad,
        customers.customer_first_name as givenname,
        customers.customer_last_name as surname
    from orders 
    
    left join payments 
    on orders.order_id = payments.order_id

    left join {{ ref("stg_jaffle_shop_customers") }} as customers
    on orders.customer_id = customers.customer_id 

    group by 1,2,3,4,5,6,7,8,10, 11
        
    order by payments.order_id
)

select * from paid_orders