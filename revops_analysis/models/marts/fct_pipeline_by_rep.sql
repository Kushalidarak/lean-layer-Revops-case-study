with snapshot as (
    select * from {{ ref('stg_deals_snapshot') }}
),

owners as (
    select * from {{ ref('stg_owners') }}
),

deals as (
    select * from {{ ref('stg_deals_meta') }}
),

current_pipeline as (
    select
        s.deal_id,
        s.owner_id,
        s.stage,
        s.forecast_category,
        s.amount,
        o.owner_name,
        o.sales_segment,
        o.is_active                             as rep_is_active,

        round(
            s.amount * case s.forecast_category
                when 'Commit'       then 0.90
                when 'Best Case'    then 0.50
                when 'Pipeline'     then 0.20
                else 0.10
            end
        , 0)                                    as weighted_amount

    from snapshot s
    left join deals d
        on s.deal_id = d.deal_id
    left join owners o
        on s.owner_id = o.owner_id

    where s.is_latest_snapshot = true
    and s.is_open = true
),

rep_summary as (
    select
        owner_id,
        owner_name,
        sales_segment,
        rep_is_active,
        forecast_category,
        count(distinct deal_id)                 as total_deals,
        sum(amount)                             as total_pipeline,
        sum(weighted_amount)                    as weighted_pipeline,
        round(avg(amount), 0)                   as avg_deal_size
    from current_pipeline
    group by
        owner_id,
        owner_name,
        sales_segment,
        rep_is_active,
        forecast_category
)

select * from rep_summary
order by total_pipeline desc