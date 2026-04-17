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
        d.deal_source,
        d.deal_type,
        o.owner_name,
        o.sales_segment,
        o.is_active                             as rep_is_active

    from snapshot s
    left join deals d
        on s.deal_id = d.deal_id
    left join owners o
        on s.owner_id = o.owner_id

    where s.is_latest_snapshot = true
    and s.is_open = true
),

forecast_summary as (
    select
        forecast_category,
        count(distinct deal_id)                 as total_deals,
        sum(amount)                             as total_pipeline,
        round(avg(amount), 0)                   as avg_deal_size,
        min(amount)                             as min_deal_size,
        max(amount)                             as max_deal_size
    from current_pipeline
    group by forecast_category
    order by total_pipeline desc
),

stage_summary as (
    select
        stage,
        count(distinct deal_id)                 as total_deals,
        sum(amount)                             as total_pipeline
    from current_pipeline
    group by stage
    order by total_pipeline desc
),

owner_summary as (
    select
        owner_id,
        owner_name,
        sales_segment,
        rep_is_active,
        count(distinct deal_id)                 as total_deals,
        sum(amount)                             as total_pipeline,
        countif(forecast_category = 'Commit')   as commit_deals,
        sum(case
            when forecast_category = 'Commit'
            then amount else 0
        end)                                    as commit_pipeline,
        countif(
            forecast_category = 'Best Case'
        )                                       as best_case_deals,
        sum(case
            when forecast_category = 'Best Case'
            then amount else 0
        end)                                    as best_case_pipeline
    from current_pipeline
    group by
        owner_id,
        owner_name,
        sales_segment,
        rep_is_active
),

pipeline_health as (
    select
        count(distinct deal_id)                 as total_open_deals,
        sum(amount)                             as total_pipeline,
        sum(case
            when forecast_category = 'Commit'
            then amount else 0
        end)                                    as commit_pipeline,
        sum(case
            when forecast_category = 'Best Case'
            then amount else 0
        end)                                    as best_case_pipeline,
        sum(case
            when forecast_category = 'Pipeline'
            then amount else 0
        end)                                    as pipeline_pipeline,
        sum(case
            when forecast_category in ('Commit', 'Best Case')
            then amount else 0
        end)                                    as realistic_forecast
    from current_pipeline
)

select * from pipeline_health