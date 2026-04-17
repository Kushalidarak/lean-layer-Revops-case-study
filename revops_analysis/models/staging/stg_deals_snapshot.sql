with source as (
    select * from {{ source('bronze', 'deals_snapshot') }}
),

cleaned as (
    select
        deal_id,
        owner_id,

        cast(snapshot_date as date)             as snapshot_date,
        cast(close_date as date)                as close_date,

        stage,
        forecast_category,

        cast(amount as numeric)                 as amount,

        case
            when stage = 'Closed Won' then true
            else false
        end                                     as is_won,

        case
            when stage = 'Closed Lost' then true
            else false
        end                                     as is_lost,

        case
            when stage not in ('Closed Won', 'Closed Lost') then true
            else false
        end                                     as is_open,
        
        case
            when cast(snapshot_date as date) = '2025-06-09' then true
            else false
        end                                     as is_latest_snapshot,

        case
            when cast(snapshot_date as date) >= '2025-01-01'
            and cast(snapshot_date as date) <= '2025-03-31'
            then true
            else false
        end                                     as is_q1_2025,

        case
            when cast(snapshot_date as date) >= '2025-04-01'
            and cast(snapshot_date as date) <= '2025-06-30'
            then true
            else false
        end                                     as is_q2_2025

    from source
    where deal_id is not null
    and snapshot_date is not null
)

select * from cleaned