with source as (
    select * from {{ source('bronze', 'deals_meta') }}
),

cleaned as (
    select
        deal_id,

        owner_id,

        deal_stage,
        forecast_category,
        record_type,
        deal_type,
        deal_order_type,

        coalesce(account_industry, 'Unknown')   as account_industry,
        coalesce(account_region, 'Unknown')     as account_region,
        coalesce(account_size, 'Unknown')       as account_size,
        coalesce(deal_source, 'Unknown')        as deal_source,

        cast(deal_amount as numeric)            as deal_amount,

        cast(created_date as date)              as created_date,
        cast(close_date as date)                as close_date,

        date_diff(
            cast(close_date as date),
            cast(created_date as date),
            day
        )                                       as deal_age_days,

        case
            when deal_stage = 'Closed Won' then true
            else false
        end                                     as is_won,

        case
            when deal_stage = 'Closed Lost' then true
            else false
        end                                     as is_lost,

        case
            when deal_stage not in ('Closed Won', 'Closed Lost') then true
            else false
        end                                     as is_open,

        -- Quarter flag for Q1 2025 analysis
        case
            when cast(close_date as date) >= '2025-01-01'
            and cast(close_date as date) <= '2025-03-31'
            then true
            else false
        end                                     as is_q1_2025

    from source
    where deal_id is not null
)

select * from cleaned