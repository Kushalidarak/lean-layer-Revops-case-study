with snapshot as (
    select * from {{ ref('stg_deals_snapshot') }}
),

deals as (
    select * from {{ ref('stg_deals_meta') }}
),

stage_order as (
    select
        deal_id,
        stage,
        snapshot_date,
        row_number() over (
            partition by deal_id, stage
            order by snapshot_date
        )                                       as stage_row_num
    from snapshot
),

first_appearance as (
    select
        deal_id,
        stage,
        snapshot_date                           as first_seen_date
    from stage_order
    where stage_row_num = 1
),

stage_counts as (
    select
        stage,
        count(distinct deal_id)                 as deals_entered
    from first_appearance
    group by stage
),

funnel_stages as (
    select
        'Prospecting'                           as from_stage,
        'Qualification'                         as to_stage,
        pro.deals_entered                       as deals_in,
        qual.deals_entered                      as deals_out,
        round(
            safe_divide(qual.deals_entered, pro.deals_entered) * 100
        , 1)                                    as conversion_rate_pct
    from stage_counts pro
    cross join stage_counts qual
    where pro.stage = 'Prospecting'
    and qual.stage = 'Qualification'

    union all

    select
        'Qualification'                         as from_stage,
        'Proposal'                              as to_stage,
        qual.deals_entered                      as deals_in,
        prop.deals_entered                      as deals_out,
        round(
            safe_divide(prop.deals_entered, qual.deals_entered) * 100
        , 1)                                    as conversion_rate_pct
    from stage_counts qual
    cross join stage_counts prop
    where qual.stage = 'Qualification'
    and prop.stage = 'Proposal'

    union all

    select
        'Proposal'                              as from_stage,
        'Negotiation'                           as to_stage,
        prop.deals_entered                      as deals_in,
        neg.deals_entered                       as deals_out,
        round(
            safe_divide(neg.deals_entered, prop.deals_entered) * 100
        , 1)                                    as conversion_rate_pct
    from stage_counts prop
    cross join stage_counts neg
    where prop.stage = 'Proposal'
    and neg.stage = 'Negotiation'

    union all

    select
        'Negotiation'                           as from_stage,
        'Closed Won'                            as to_stage,
        neg.deals_entered                       as deals_in,
        won.deals_entered                       as deals_out,
        round(
            safe_divide(won.deals_entered, neg.deals_entered) * 100
        , 1)                                    as conversion_rate_pct
    from stage_counts neg
    cross join stage_counts won
    where neg.stage = 'Negotiation'
    and won.stage = 'Closed Won'
)

select
    from_stage,
    to_stage,
    deals_in,
    deals_out,
    conversion_rate_pct,
    100 - conversion_rate_pct                  as drop_off_rate_pct
from funnel_stages
order by
    case from_stage
        when 'Prospecting' then 1
        when 'Qualification' then 2
        when 'Proposal' then 3
        when 'Negotiation' then 4
    end