MERGE `project_name.gcsd_dm.tb_gcs_user_history` target

USING

(
 WITH distinct_data AS
    -- new rows added to user
    (select j.*,
    datetime(timestamp(u.createddate)) as createddate,
    datetime(timestamp(u.lastmodifieddate)) as lastmodifieddate
    from (
                select coalesce(id,'') as id, coalesce(managerid,'') as managerid, coalesce(userroleid,'') as userroleid,
                coalesce(theatre__c,'') as theatre, coalesce(support_engineer_location__c,'') as support_engineer_location__c,
                case when isactive is true then "true" else "false" end as isactive,
                coalesce(usertype,'') as usertype,coalesce(my_sales_level__c,'') as my_sales_level__c,coalesce(PROFILEID,'') as PROFILEID,
                coalesce(SEGMENT__C,'') as SEGMENT__C,coalesce(AREA__C,'') as AREA__C,coalesce(region__c,'') as region,
                coalesce(district__c,'') as district ,coalesce(division,'') as division,coalesce(department,'') as department
                from `project_name.sfdc.user` -- latest user feed

                except distinct

              select coalesce(id,'') as id, coalesce(managerid,'') as managerid, coalesce(userroleid,'') as userroleid,
              coalesce(theatre,'') as theatre, coalesce(support_engineer_location__c,'') as support_engineer_location__c,
                coalesce(isactive,'') as isactive,coalesce(usertype,'') as usertype,coalesce(my_sales_level__c,'') as my_sales_level__c,
                coalesce(PROFILEID,'') as PROFILEID ,coalesce(SEGMENT__C,'') as SEGMENT__C,coalesce(AREA__C,'') as AREA__C,
                coalesce(region,'')  as region,coalesce(district,'') as district,coalesce(division,'')  as division,coalesce(department,'') as department
                from `project_name.gcsd_dm.tb_gcs_user_history`
                where todate = '9999-12-31T23:59:59'
              ) j

    left outer join `project_name.sfdc.user` u

    on j.id = u.id
    )

select  id,NULLIF(managerid,'') as managerid,NULLIF(userroleid,'') as userroleid,NULLIF(theatre,'') as theatre,NULLIF(support_engineer_location__c,'') as support_engineer_location__c,
NULLIF(isactive,'') as isactive,NULLIF(usertype,'') as usertype,NULLIF(my_sales_level__c,'') as my_sales_level__c,NULLIF(PROFILEID,'') as PROFILEID,NULLIF(SEGMENT__C,'') as SEGMENT__C,
NULLIF(AREA__C,'') as AREA__C,NULLIF(region,'') as region,NULLIF(district,'') as district,NULLIF(division,'') as division,NULLIF(department,'') as department,
createddate,lastmodifieddate,
fromdate_new as fromdate,todate_new as todate from (
  select * except(fromdate,todate),fromdate as fromdate_old,todate as todate_old,

   case
      when lastmodifieddate = fromdate
        then fromdate
      when createddate is null
        then fromdate
      when fromdate is null        
       -- BG-3183: removed this --then datetime_add(lastmodifieddate, INTERVAL 1 SECOND)
      then lastmodifieddate
      end as fromdate_new,

   case
    when
      ifnull(datetime_sub(lead(lastmodifieddate,1) over (partition by id order by lastmodifieddate),INTERVAL 1 SECOND),
      datetime(timestamp(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S', '9999-12-31T23:59:59'))))  < lastmodifieddate
    then
      lastmodifieddate
    else
      ifnull(datetime_sub(lead(lastmodifieddate,1) over (partition by id order by lastmodifieddate),INTERVAL 1 SECOND),
      datetime(timestamp(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S', '9999-12-31T23:59:59'))))
    end as todate_new

    from
        (-- latest rows from snapshot (type2)
          select id, managerid, userroleid, theatre, support_engineer_location__c, isactive,usertype,my_sales_level__c,PROFILEID,SEGMENT__C,AREA__C,
                 region ,district ,division ,department,
                 null as createddate,
                 datetime(timestamp(lastmodifieddate)) as lastmodifieddate,
                 fromdate,todate
                 from `project_name.gcsd_dm.tb_gcs_user_history`
                 where todate = '9999-12-31T23:59:59'
                 and id in (select id from distinct_data)

          union all

          select *,null as fromdate,null as todate from distinct_data
        )
  )
  where  fromdate_new<todate_new
) source

ON target.id = source.id
AND target.fromdate = source.fromdate
AND target.todate = '9999-12-31T23:59:59'

WHEN NOT MATCHED BY target
THEN
INSERT (id, managerid, userroleid, theatre, support_engineer_location__c, isactive,usertype,my_sales_level__c,
        PROFILEID,SEGMENT__C,AREA__C,region ,district ,division ,department,
        fromdate,todate ,lastmodifieddate,loadtimestamp,userdeltaloadts)
VALUES(source.id, source.managerid, source.userroleid, source.theatre, source.support_engineer_location__c,
        cast(source.isactive as string),source.usertype,source.my_sales_level__c,source.PROFILEID,source.SEGMENT__C,source.AREA__C,
        source.region,source.district,source.division ,source.department,
        source.fromdate, source.todate, source.lastmodifieddate,
        datetime(timestamp(substr(cast (current_timestamp() as string),0,19))),
        datetime(timestamp(substr(cast (current_timestamp() as string),0,19))))

-- matched
WHEN MATCHED
THEN
UPDATE SET target.todate = source.todate
