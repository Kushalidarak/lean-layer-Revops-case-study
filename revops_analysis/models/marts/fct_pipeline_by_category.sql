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
        s.close_date,
        d.account_industry,
        d.account_region,
        d.account_size,
        d.deal_type,
        o.owner_name,
        o.sales_segment,
        o.is_active                             as rep_is_active,

        case s.forecast_category
            when 'Commit'       then 0.90
            when 'Best Case'    then 0.50
            when 'Pipeline'     then 0.20
            else 0.10
        end                                     as win_probability,

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

category_summary as (
    select
        forecast_category,
        count(distinct deal_id)                 as total_deals,
        sum(amount)                             as total_pipeline,
        sum(weighted_amount)                    as weighted_pipeline,
        round(avg(amount), 0)                   as avg_deal_size
    from current_pipeline
    group by forecast_category
)

select * from category_summary
order by
    case forecast_category
        when 'Commit' then 1
        when 'Best Case' then 2
        when 'Pipeline' then 3
        else 4
    end