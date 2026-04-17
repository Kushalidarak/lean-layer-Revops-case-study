with source as (
    select * from {{ source('bronze', 'owners') }}
),

cleaned as (
    select
        owner_id,
        name                                    as owner_name,
        email                                   as owner_email,
        role                                    as owner_role,
        manager                                 as manager_name,
        segment                                 as sales_segment,

        cast(role_start_date as date)           as role_start_date,
        cast(role_end_date as date)             as role_end_date,

    
        case
            when role_end_date is null then true
            else false
        end                                     as is_active,

        date_diff(
            coalesce(cast(role_end_date as date), current_date()),
            cast(role_start_date as date),
            day
        )                                       as tenure_days

    from source
    where owner_id is not null
)

select * from cleaned