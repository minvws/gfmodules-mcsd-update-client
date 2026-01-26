alter table resource_maps
    drop column supplier_resource_version;

alter table resource_maps
    drop column consumer_resource_version;

alter table resource_maps
    add history_size integer default 0 not null;

