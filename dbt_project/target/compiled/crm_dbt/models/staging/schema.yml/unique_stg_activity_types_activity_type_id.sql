
    
    

select
    activity_type_id as unique_field,
    count(*) as n_records

from `crm_staging`.`stg_activity_types`
where activity_type_id is not null
group by activity_type_id
having count(*) > 1


