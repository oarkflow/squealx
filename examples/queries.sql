-- sql-name: list-cpt
-- doc: Get CPT codes
-- connection: test


SELECT charge_master_id, work_item_id, cpt_hcpcs_code,client_proc_desc
FROM charge_master WHERE work_item_id = @work_item_id LIMIT {{if work_item_id == 33}} 1 {{else}} 10 {{end}};

-- sql-end

-- sql-name: list-persons
-- doc: Get All persons
-- connection: test


SELECT * FROM persons LIMIT 10;

-- sql-end