with deals as (
    select * from {{ ref('stg_deals_meta') }}
),

owners as (
    select * from {{ ref('stg_owners') }}
),

q1_deals as (
    select
        d.deal_id,
        d.owner_id,
        d.deal_stage,
        d.deal_amount,
        d.account_industry,
        d.account_region,
        d.account_size,
        d.deal_source,
        d.deal_type,
        d.close_date,
        d.is_won,
        d.is_lost,
        d.is_open,
        d.deal_age_days,
        o.owner_name,
        o.sales_segment,
        o.is_active                             as rep_is_active

    from deals d
    left join owners o
        on d.owner_id = o.owner_id

    where d.close_date >= '2025-01-01'
    and d.close_date <= '2025-03-31'
),


owner_summary as (
    select
        owner_id,
        owner_name,
        sales_segment,
        rep_is_active,
        countif(is_won)                         as deals_won,
        countif(is_lost)                        as deals_lost,
        sum(case when is_won then deal_amount
            else 0 end)                         as won_revenue,
        sum(case when is_lost then deal_amount
            else 0 end)                         as lost_revenue,
        round(avg(deal_age_days), 0)            as avg_deal_age_days
    from q1_deals
    group by
        owner_id,
        owner_name,
        sales_segment,
        rep_is_active
),

industry_region_summary as (
    select
        account_industry,
        account_region,
        countif(is_won)                         as deals_won,
        countif(is_lost)                        as deals_lost,
        sum(case when is_won then deal_amount
            else 0 end)                         as won_revenue,
        sum(case when is_lost then deal_amount
            else 0 end)                         as lost_revenue
    from q1_deals
    group by
        account_industry,
        account_region
),

overall_summary as (
    select
        countif(is_won)                         as total_deals_won,
        countif(is_lost)                        as total_deals_lost,
        sum(case when is_won then deal_amount
            else 0 end)                         as total_won_revenue,
        sum(case when is_lost then deal_amount
            else 0 end)                         as total_lost_revenue,
        sum(deal_amount)                        as total_pipeline_revenue
    from q1_deals
)

select * from q1_deals