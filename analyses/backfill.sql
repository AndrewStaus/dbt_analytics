with hits as (
    select *
    from (
       values 
        ('2024-01-01 00:00:00', 11111, 'summer_sale'), 
        ('2024-01-01 00:02:00', 11111, null),
        ('2024-01-01 00:03:00', 11111, 'winter_sale'),
        ('2024-01-01 00:04:00', 11111, 'spring_sale'),
        ('2024-01-01 00:00:00', 22222, 'summer_sale'),
        ('2024-01-01 00:02:00', 22222, null),
        ('2024-01-01 00:03:00', 22222, null),
        ('2024-01-01 00:04:00', 22222, 'spring_sale'),
        ('2024-01-01 00:05:00', 22222, null),
        ('2024-01-01 00:00:00', 33333, null),
        ('2024-01-01 00:02:00', 33333, null),
        ('2024-01-01 00:03:00', 33333, null),
        ('2024-01-01 00:04:00', 33333, 'spring_sale')
    ) as t (date_time, entity_id, campaign)
),

x as (
    select
        *,
        nvl(campaign,
            first_value(campaign) ignore nulls over (
                partition by entity_id
                order by date_time
            )
        ) campaign2,
        case when campaign is null
            then null
            else date_time
        end date_time2
    from hits
)

select distinct
    entity_id,
    campaign2 campaign,
    nvl(date_time2,
        first_value(date_time2) ignore nulls over (
            partition by entity_id
            order by date_time
        )
    ) start_time,
from x
where campaign2 is not null
order by entity_id, start_time