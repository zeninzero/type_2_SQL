--------------------------------------------
-- delta upsert CASE HOT
--------------------------------------------

-- ** destination table name variable
MERGE `project_name.gcsd_dm.tb_gcs_case_hot_cases_history` target

USING
-- source snippet
(

WITH master_data AS
(
  select *,row_number() over (partition by parent_id order by createddate) as rn
  from
  (
      select *,case when parent_createddate_gmt = ch.createddate then 'Y' else 'N' end as toDelete 
       from (select id as parent_id,createddate as parent_createddate_gmt
-- ** attribute variable
                      ,hot_case__c
                      from `project_name.sfdc.case`
-- ** start date window variable
                      where lastmodifieddate >= (select max(fromdate) from `project_name.gcsd_dm.tb_gcs_case_hot_cases_history`)
                      ) parent

       left outer join

       (select id,caseid,oldvalue,newvalue,createddate
          from
            (
              select id as oldid,caseid,oldvalue,newvalue,createddate
              ,min(id) over (partition by caseid,createddate) as id
              from `project_name.sfdc.casehistory`
-- ** attribute variable
              where field = 'Hot_Case__c'
-- ** start date window variable
            ) where oldid=id and createddate >= (select max(fromdate) from `project_name.gcsd_dm.tb_gcs_case_hot_cases_history`)
        ) ch

      on ch.caseid = parent.parent_id
  ) where toDelete = 'N'
)

select max(id) as id,caseid,hot_case__c,min(start_dt) as fromdate,
datetime(timestamp(substr(cast(max(end_dt) as string),0,19))) as todate,
current_timestamp() as loadtimestamp
from (
select *
    ,row_number() over (partition by caseid, hot_case__c order by start_dt) as seqnum_t
    ,row_number() OVER (partition by caseid order by start_dt) as seqnum
    from (
          select ifnull(id,'099900000XxxYyyXXX') as id,
          parent_id as caseid,
          ifnull(oldvalue,cast(hot_case__c as string)) as hot_case__c,
          parent_createddate_gmt as start_dt,
          case when (createddate is null) then (PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '9999-12-31 23:59:59'))
          else TIMESTAMP_SUB(timestamp((createddate)), INTERVAL 1 SECOND) end as end_dt
          from master_data where rn = 1

          union all

          (
            select id,parent_id as caseid,newvalue,createddate as start_dt,
            timestamp(ifnull(datetime_sub(datetime(timestamp((lead(createddate,1) over (partition by caseid order by createddate)))),INTERVAL 1 SECOND),
            PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '9999-12-31 23:59:59'))) AS end_dt
            from master_data
          )

 ) where id is not null
)
group by caseid,hot_case__c,(seqnum - seqnum_t)
order by fromdate

) source

-- on merge condition
-- caseid same, hot_case__c same, start dt smaller
-- then update else insert
ON target.caseid = source.caseid
AND target.todate = '9999-12-31 23:59:59'
AND target.fromdate >= source.fromdate

--not_matched_by_target_clause
WHEN NOT MATCHED BY target
THEN
-- ** insert values variablize : hot_case__c
INSERT (id,caseid,hot_case__c,fromdate,todate,loadtimestamp)
VALUES(source.id,source.caseid,source.hot_case__c,source.fromdate,source.todate,source.loadtimestamp)

-- matched
WHEN MATCHED
THEN
-- ** update values variablize : hot_case__c
UPDATE SET target.todate = source.todate , target.hot_case__c=source.hot_case__c
