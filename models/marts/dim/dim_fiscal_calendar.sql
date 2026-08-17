{{
    config(
        materialized='table',
        tags=['goal', 'dim']
    )
}}

/*
    Fiscal calendar covering the goal-planning horizon.

    Fiscal year is aligned to the calendar year here; change the quarter
    derivation below if the business runs on an offset year. Everything that
    depends on pacing reads workday_index_in_period and workdays_in_period
    from this model, so the offset only has to be corrected in one place.
*/

{% set calendar_start = var('fiscal_calendar_start', '2024-01-01') %}
{% set calendar_days = var('fiscal_calendar_days', 1461) %}

with date_spine as (

    select
        dateadd(day, seq4(), to_date('{{ calendar_start }}')) as calendar_date
    from table(generator(rowcount => {{ calendar_days }}))

),

enriched as (

    select
        calendar_date,
        year(calendar_date) as fiscal_year,
        quarter(calendar_date) as fiscal_quarter,
        month(calendar_date) as fiscal_month,
        weekofyear(calendar_date) as fiscal_week,
        dayofweekiso(calendar_date) as iso_day_of_week,
        'FY' || right(cast(year(calendar_date) as varchar), 2)
            || '-Q' || cast(quarter(calendar_date) as varchar) as fiscal_period_id,
        'FY' || right(cast(year(calendar_date) as varchar), 2)
            || '-M' || lpad(cast(month(calendar_date) as varchar), 2, '0') as fiscal_month_id,
        case when dayofweekiso(calendar_date) <= 5 then 1 else 0 end as is_workday
    from date_spine

),

period_bounds as (

    select
        fiscal_period_id,
        min(calendar_date) as period_start_date,
        max(calendar_date) as period_end_date,
        sum(is_workday) as workdays_in_period,
        count(*) as days_in_period
    from enriched
    group by fiscal_period_id

)

select
    e.calendar_date,
    e.fiscal_period_id,
    e.fiscal_month_id,
    e.fiscal_year,
    e.fiscal_quarter,
    e.fiscal_month,
    e.fiscal_week,
    e.iso_day_of_week,
    e.is_workday,
    b.period_start_date,
    b.period_end_date,
    b.days_in_period,
    b.workdays_in_period,
    datediff(day, b.period_start_date, e.calendar_date) + 1 as day_index_in_period,
    sum(e.is_workday) over (
        partition by e.fiscal_period_id
        order by e.calendar_date
        rows between unbounded preceding and current row
    ) as workday_index_in_period,
    /*
        Linear pacing baseline. Replace with a fitted seasonality curve once
        enough closed periods exist — most B2B quarters land well behind
        linear at the midpoint and catch up in the final two weeks, so a linear
        expectation flags healthy quarters as AT_RISK on day 45.
    */
    round(
        sum(e.is_workday) over (
            partition by e.fiscal_period_id
            order by e.calendar_date
            rows between unbounded preceding and current row
        ) / nullif(b.workdays_in_period, 0),
        4
    ) as expected_attainment_pct_linear
from enriched as e
inner join period_bounds as b
    on e.fiscal_period_id = b.fiscal_period_id
