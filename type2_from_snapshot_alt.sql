with master_data as
(
select * except (rn),
from (
      select *,row_number() over (partition by emp_id,cost_center_code  order by snapshot_date asc) as rn,
      datetime_add(lastmodifieddate,interval 1 second) as start_dt
      from (
            select
            *,
            date(bi_snapshot_timestamp) as snapshot_date,
            datetime(timestamp(substr(cast(bi_snapshot_timestamp as string),0,19))) as lastmodifieddate,
            from `snapshot.emp_data`
            order by snapshot_date desc
            )
      ) where rn = 1
)

select *,
ifnull(LEAD(lastmodifieddate,1) over (partition by emp_id  order by lastmodifieddate asc),
datetime(timestamp('9999-12-31T23:59:59'))) as end_dt
from (
      select * except (rn)
      from
      (select *,row_number() over (partition by emp_id  order by snapshot_date asc) as rn
      from (
            select * except (lastmodifieddate ,start_dt),
            datetime(hire_date) as lastmodifieddate,
            datetime(hire_date) as start_dt      
            from master_data
      ) ) where rn = 1      

      union all

      select *
      from master_data

      order by start_dt asc
     )
