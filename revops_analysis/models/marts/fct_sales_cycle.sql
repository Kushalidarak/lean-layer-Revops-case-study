with deals as (
    select * from {{ ref('stg_deals_meta') }}
),

owners as (
    select * from {{ ref('stg_owners') }}
),

closed_deals as (
    select
        d.deal_id,
        d.owner_id,
        d.deal_stage,
        d.deal_amount,
        d.deal_type,
        d.deal_order_type,
        d.account_industry,
        d.account_region,
        d.account_size,
        d.deal_source,
        d.created_date,
        d.close_date,
        d.deal_age_days,
        d.is_won,
        d.is_lost,
        o.owner_name,
        o.sales_segment

    from deals d
    left join owners o
        on d.owner_id = o.owner_id

    where d.deal_stage in ('Closed Won', 'Closed Lost')
    and d.deal_age_days is not null
    and d.deal_age_days > 0
),

segment_summary as (
    select
        sales_segment,
        deal_type,
        count(distinct deal_id)                 as total_deals,
        countif(is_won)                         as deals_won,
        countif(is_lost)                        as deals_lost,
        round(avg(deal_age_days), 0)            as avg_days_to_close,
        min(deal_age_days)                      as min_days_to_close,
        max(deal_age_days)                      as max_days_to_close,
        round(avg(
            case when is_won then deal_age_days end
        ), 0)                                   as avg_days_won_deals,
        round(avg(
            case when is_lost then deal_age_days end
        ), 0)                                   as avg_days_lost_deals,
        round(avg(deal_amount), 0)              as avg_deal_size,
        round(
            safe_divide(
                countif(is_won),
                count(distinct deal_id)
            ) * 100
        , 1)                                    as win_rate_pct
    from closed_deals
    group by
        sales_segment,
        deal_type
),

industry_summary as (
    select
        account_industry,
        account_region,
        count(distinct deal_id)                 as total_deals,
        countif(is_won)                         as deals_won,
        countif(is_lost)                        as deals_lost,
        round(avg(deal_age_days), 0)            as avg_days_to_close,
        round(avg(deal_amount), 0)              as avg_deal_size,
        round(
            safe_divide(
                countif(is_won),
                count(distinct deal_id)
            ) * 100
        , 1)                                    as win_rate_pct
    from closed_deals
    group by
        account_industry,
        account_region
)

select * from segment_summary
order by avg_days_to_close desc