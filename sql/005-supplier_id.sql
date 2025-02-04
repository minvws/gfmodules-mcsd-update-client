drop table suppliers;

alter table resource_maps
    alter column supplier_id type uuid using supplier_id::uuid;


