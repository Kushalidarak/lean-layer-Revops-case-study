with source as (
    select * from {{ source('bronze', 'targets') }}
),

cleaned as (
    select
        
        owner_id,

        
        quarter                                 as quarter_label,
        segment,

        -
        cast(
            split(quarter, ' ')[offset(0)]
            as int64
        )                                       as fiscal_year,

        cast(
            replace(
                split(quarter, ' ')[offset(1)],
                'Q', ''
            )
            as int64
        )                                       as fiscal_quarter,

       
        cast(target_amount as numeric)          as target_amount,

       
        case
            when cast(target_amount as numeric) > 1000000
            then true
            else false
        end                                     as is_unrealistic_target

    from source
    where owner_id is not null
)

select * from cleaned