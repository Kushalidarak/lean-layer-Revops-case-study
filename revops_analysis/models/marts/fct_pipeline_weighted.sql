with pipeline as (
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
        p.deal_id,
        p.owner_id,
        p.stage,
        p.forecast_category,
        p.amount,
        p.close_date,
        d.account_industry,
        d.account_region,
        d.account_size,
        d.deal_source,
        d.deal_type,
        o.owner_name,
        o.sales_segment,
        o.is_active                             as rep_is_active,

        
        case p.forecast_category
            when 'Commit'       then 0.90
            when 'Best Case'    then 0.50
            when 'Pipeline'     then 0.20
            else 0.10
        end                                     as win_probability,

        
        round(
            p.amount * case p.forecast_category
                when 'Commit'       then 0.90
                when 'Best Case'    then 0.50
                when 'Pipeline'     then 0.20
                else 0.10
            end
        , 0)                                    as weighted_amount,

        
        date_diff(
            cast(p.close_date as date),
            current_date(),
            day
        )                                       as days_to_close,

        
        case
            when date_diff(
                cast(p.close_date as date),
                current_date(),
                day
            ) < 0 then 'Overdue'
            when date_diff(
                cast(p.close_date as date),
                current_date(),
                day
            ) <= 30 then 'Closing Soon'
            when date_diff(
                cast(p.close_date as date),
                current_date(),
                day
            ) <= 90 then 'On Track'
            else 'Future'
        end                                     as close_date_risk,

        
        case
            when cast(p.close_date as date) >= '2025-04-01'
            and cast(p.close_date as date) <= '2025-06-30'
            then true
            else false
        end                                     as is_q2_close_date

    from pipeline p
    left join deals d
        on p.deal_id = d.deal_id
    left join owners o
        on p.owner_id = o.owner_id

    where p.is_latest_snapshot = true
    and p.is_open = true
),

summary as (
    select
        -- Overall pipeline metrics
        count(distinct deal_id)                 as total_open_deals,
        sum(amount)                             as total_pipeline,
        sum(weighted_amount)                    as total_weighted_pipeline,

        -- By forecast category
        sum(case when forecast_category = 'Commit'
            then amount else 0 end)             as commit_pipeline,
        sum(case when forecast_category = 'Best Case'
            then amount else 0 end)             as best_case_pipeline,
        sum(case when forecast_category = 'Pipeline'
            then amount else 0 end)             as pipeline_pipeline,

        -- Weighted by category
        sum(case when forecast_category = 'Commit'
            then weighted_amount else 0 end)    as commit_weighted,
        sum(case when forecast_category = 'Best Case'
            then weighted_amount else 0 end)    as best_case_weighted,
        sum(case when forecast_category = 'Pipeline'
            then weighted_amount else 0 end)    as pipeline_weighted,

        
        sum(case when forecast_category in ('Commit', 'Best Case')
            then amount else 0 end)             as realistic_forecast,

        
        sum(weighted_amount)                    as weighted_forecast,

        
        countif(close_date_risk = 'Overdue')    as overdue_deals,
        countif(close_date_risk = 'Closing Soon') as closing_soon_deals,
        countif(not rep_is_active)              as orphaned_deals,

        
        sum(case when is_q2_close_date
            then amount else 0 end)             as q2_targeted_pipeline,
        sum(case when is_q2_close_date
            then weighted_amount else 0 end)    as q2_weighted_pipeline,

        
        round(
            safe_divide(
                sum(amount),
                949347
            )
        , 1)                                    as pipeline_coverage_ratio

    from current_pipeline
)

select * from summary