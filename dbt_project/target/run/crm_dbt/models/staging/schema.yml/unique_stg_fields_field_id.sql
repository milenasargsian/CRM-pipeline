
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    field_id as unique_field,
    count(*) as n_records

from `crm_staging`.`stg_fields`
where field_id is not null
group by field_id
having count(*) > 1



  
  
    ) dbt_internal_test