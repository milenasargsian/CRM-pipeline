
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select change_time
from `crm_staging`.`stg_deal_changes`
where change_time is null



  
  
    ) dbt_internal_test