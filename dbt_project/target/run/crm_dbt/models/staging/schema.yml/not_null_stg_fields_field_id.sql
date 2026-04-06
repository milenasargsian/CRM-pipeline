
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select field_id
from `crm_staging`.`stg_fields`
where field_id is null



  
  
    ) dbt_internal_test