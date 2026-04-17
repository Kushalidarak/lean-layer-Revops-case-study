with snapshot as (
    select * from {{ ref('stg_deals_snapshot') }}
),

deals as (
    select * from {{ ref('stg_deals_meta') }}
),

owners as (
    select * from {{ ref('stg_owners') }}
),

stage_duration as (
    select
        deal_id,
        owner_id,
        stage,
        count(*)                                as weeks_in_stage,
        min(snapshot_date)                      as stage_entry_date,
        max(snapshot_date)                      as stage_exit_date
    from snapshot
    group by
        deal_id,
        owner_id,
        stage
),

stage_summary as (
    select
        stage,
        count(distinct deal_id)                 as total_deals,
        round(avg(weeks_in_stage), 1)           as avg_weeks_in_stage,
        max(weeks_in_stage)                     as max_weeks_stuck,
        min(weeks_in_stage)                     as min_weeks_in_stage,
        round(sum(weeks_in_stage), 0)           as total_weeks_spent
    from stage_duration
    where stage not in ('Closed Won', 'Closed Lost')
    group by stage
    order by avg_weeks_in_stage desc
),

current_pipeline as (
    select
        s.stage,
        s.deal_id,
        s.owner_id,
        s.amount,
        s.forecast_category,
        d.account_industry,
        d.account_region,
        d.account_size,
        o.owner_name,
        o.sales_segment
    from snapshot s
    left join deals d
        on s.deal_id = d.deal_id
    left join owners o
        on s.owner_id = o.owner_id
    where s.is_latest_snapshot = true
    and s.is_open = true
),

final as (
    select
        ss.stage,
        ss.total_deals,
        ss.avg_weeks_in_stage,
        ss.max_weeks_stuck,
        ss.min_weeks_in_stage,
        ss.total_weeks_spent
    from stage_summary ss
)

select * from final