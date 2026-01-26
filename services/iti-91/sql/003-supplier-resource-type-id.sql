alter table resource_maps
  drop constraint resource_maps_supplier_id_supplier_resource_id_key;

alter table resource_maps
    add unique (supplier_id, resource_type, supplier_resource_id);
