-- sql-name: list-cpt
-- doc: Get CPT codes
-- connection: test


SELECT charge_master_id, work_item_id, cpt_hcpcs_code,client_proc_desc
FROM charge_master LIMIT 10;

-- sql-end

-- sql-name: list-persons
-- doc: Get All persons
-- connection: test


SELECT * FROM persons LIMIT 10;

-- sql-end