
CREATE INDEX charge_master_ibfk_2 ON charge_master (provider_category);
CREATE INDEX idx_charge_type ON charge_master (effective_date, charge_type);
CREATE INDEX idx_report_idx ON charge_master (report);
CREATE INDEX idx_wi_eff_date ON charge_master (work_item_id, effective_date);
CREATE INDEX idx_wi_type_cpt_dt ON charge_master (work_item_id, charge_type, cpt_hcpcs_code, effective_date);
CREATE INDEX idx_wi_type_dt_cpt ON charge_master (work_item_id, charge_type, effective_date, cpt_hcpcs_code);
CREATE INDEX charge_master_ibfk_1 ON charge_master (charge_type);
CREATE INDEX uniq_wi_type_date_cpt_cat_internal ON charge_master (work_item_id, charge_type, effective_date, cpt_hcpcs_code, provider_category, client_internal_code);
CREATE INDEX fk_patient_status_id ON charge_master (patient_status_id);
CREATE INDEX wi_pt_ft_hcpcs_date ON charge_master (work_item_id, profee_type, facility_type, cpt_hcpcs_code, effective_date);
CREATE INDEX encounter_hand_ibfk_1 ON encounter_hand (encounter_id);
CREATE INDEX event_em_fac_ibfk_15 ON event_em_fac (cosigning_physician);
CREATE INDEX event_em_fac_ibfk_3 ON event_em_fac (em_midlevel);
CREATE INDEX work_item_id_2 ON event_em_fac (work_item_id, encounter_id);
CREATE INDEX event_em_fac_ibfk_2 ON event_em_fac (em_provider);
CREATE INDEX event_em_fac_ibfk_4 ON event_em_fac (qa_procedure_provider);
CREATE INDEX em_level ON event_em_fac (em_level);
CREATE INDEX encounter_id ON event_em_fac (encounter_id, work_item_id, event_dos);
CREATE INDEX fk_patient_status_id ON event_em_fac (patient_status_id);
CREATE INDEX idx_enc_id ON event_em_fac (encounter_id);
CREATE INDEX mdm_total ON event_em_fac (mdm_total);
CREATE INDEX event_em_fac_ibfk_5 ON event_em_fac (userid);
CREATE INDEX event_em_fac_ibfk_6 ON event_em_fac (qa_userid);
CREATE INDEX event_special_pro_ibfk_7 ON event_special_pro (qa_procedure_provider);
CREATE INDEX event_special_pro_ibfk_8 ON event_special_pro (userid);
CREATE INDEX event_special_pro_ibfk_9 ON event_special_pro (qa_userid);
CREATE INDEX event_special_pro_ibfk_11 ON event_special_pro (encounter_id);
CREATE INDEX event_special_pro_ibfk_4 ON event_special_pro (work_item_id, encounter_id);
CREATE INDEX event_special_pro_ibfk_5 ON event_special_pro (cosigning_physician);
CREATE INDEX event_special_pro_ibfk_6 ON event_special_pro (procedure_provider);
CREATE INDEX mirth_exception_ibfk_1 ON mirth_exception (work_item_id, encounter_id);
CREATE INDEX mirth_exception_ibfk_2 ON mirth_exception (event_em_fac_id);
CREATE INDEX mirth_exception_ibfk_3 ON mirth_exception (event_cpt_fac_id);
CREATE INDEX unique_id ON notification_emails (id);
CREATE INDEX unique_id ON user_emails (id);
CREATE INDEX idx_mrn_temp ON encounters (patient_mrn);
CREATE INDEX encounter_ibfk_1 ON encounters (attend_provider_id);
CREATE INDEX idx_dos ON encounters (encounter_dos);
CREATE INDEX idx_fin ON encounters (patient_fin);
CREATE INDEX idx_ins ON encounters (encounter_primary_type);
CREATE INDEX idx_fac_id ON encounters (facility_id);
CREATE INDEX idx_name ON encounters (patient_name);
CREATE INDEX idx_prov ON encounters (encounter_assign_provider1);
CREATE INDEX cpt_fac_charge_ibfk_2 ON cpt_fac_charge (charge_master_id);
CREATE INDEX idx_encid ON encounter_history (encounter_id);
CREATE INDEX encounter_history_ibfk_2 ON encounter_history (user_id);
CREATE INDEX encounter_history_ibfk_3 ON encounter_history (work_item_id, encounter_id);
CREATE INDEX event_icd10_dx_fac_ibfk_3_archived ON event_icd10_dx_fac_archived (work_item_id, encounter_id);
CREATE INDEX event_icd10_dx_fac_ibfk_4_archived ON event_icd10_dx_fac_archived (dx_reason);
CREATE INDEX event_icd10_dx_fac_ibfk_5_archived ON event_icd10_dx_fac_archived (code);
CREATE INDEX event_icd10_dx_fac_ibfk_1_archived ON event_icd10_dx_fac_archived (userid);
CREATE INDEX event_icd10_dx_fac_ibfk_2_archived ON event_icd10_dx_fac_archived (qa_userid);
CREATE INDEX coding_type ON cdi_reason (coding_type, cdi_area, cdi_item);
CREATE INDEX event_pqri_pro_ibfk_5 ON event_pqri_pro (userid);
CREATE INDEX event_pqri_pro_ibfk_6 ON event_pqri_pro (encounter_id);
CREATE INDEX event_pqri_pro_ibfk_3 ON event_pqri_pro (qa_userid);
CREATE INDEX effective_date ON icd10 (effective_date, tbl, header, code);
CREATE INDEX idx_code ON icd10 (code);
CREATE INDEX idx_header_tbl ON icd10 (header, tbl);
CREATE INDEX idx_with_period ON icd10 (effective_date, tbl, header, code_with_period);
CREATE INDEX change_password_ibfk_1 ON change_password (user_id);
CREATE INDEX event_hcpcs_pro_ibfk_2 ON event_hcpcs_pro (hcpcs_provider);
CREATE INDEX event_hcpcs_pro_ibfk_3 ON event_hcpcs_pro (qa_procedure_provider);
CREATE INDEX event_hcpcs_pro_ibfk_5 ON event_hcpcs_pro (qa_userid);
CREATE INDEX event_hcpcs_pro_ibfk_7 ON event_hcpcs_pro (encounter_id);
CREATE INDEX event_hcpcs_pro_ibfk_9 ON event_hcpcs_pro (userid);
CREATE INDEX work_item_id_2 ON event_hcpcs_pro (work_item_id, encounter_id);
CREATE INDEX fk_patient_status_id ON event_hcpcs_pro (patient_status_id);
CREATE INDEX Event ON event_type (event);
CREATE INDEX work_item_patient_status_ibfk_1 ON work_item_patient_status (work_item_id);
CREATE INDEX event_cdi_pro_ibfk_2 ON event_cdi_pro (userid);
CREATE INDEX event_cdi_pro_ibfk_3 ON event_cdi_pro (qa_userid);
CREATE INDEX event_cdi_pro_ibfk_4 ON event_cdi_pro (encounter_id);
CREATE INDEX event_cdi_pro_ibfk_5 ON event_cdi_pro (work_item_id);
CREATE INDEX fk_patient_status_id ON event_em_pro (patient_status_id);
CREATE INDEX idx_enc_id ON event_em_pro (encounter_id);
CREATE INDEX event_em_pro_ibfk_2 ON event_em_pro (em_provider);
CREATE INDEX event_em_pro_ibfk_3 ON event_em_pro (em_midlevel);
CREATE INDEX event_em_pro_ibfk_5 ON event_em_pro (userid);
CREATE INDEX work_item_id_2 ON event_em_pro (work_item_id, encounter_id);
CREATE INDEX em_level ON event_em_pro (em_level);
CREATE INDEX encounter_id ON event_em_pro (encounter_id, work_item_id, event_dos);
CREATE INDEX mdm_total ON event_em_pro (mdm_total);
CREATE INDEX event_em_pro_ibfk_15 ON event_em_pro (cosigning_physician);
CREATE INDEX event_em_pro_ibfk_4 ON event_em_pro (qa_procedure_provider);
CREATE INDEX event_em_pro_ibfk_6 ON event_em_pro (qa_userid);
CREATE INDEX event_icd10_dx_fac_ibfk_1 ON event_icd10_dx_fac (userid);
CREATE INDEX event_icd10_dx_fac_ibfk_2 ON event_icd10_dx_fac (qa_userid);
CREATE INDEX event_icd10_dx_fac_ibfk_3 ON event_icd10_dx_fac (work_item_id, encounter_id);
CREATE INDEX event_icd10_dx_fac_ibfk_4 ON event_icd10_dx_fac (dx_reason);
CREATE INDEX event_icd10_dx_fac_ibfk_5 ON event_icd10_dx_fac (code);
CREATE INDEX event_icd10_dx_pro_ibfk_1_archived ON event_icd10_dx_pro_archived (userid);
CREATE INDEX event_icd10_dx_pro_ibfk_2_archived ON event_icd10_dx_pro_archived (qa_userid);
CREATE INDEX event_icd10_dx_pro_ibfk_3_archived ON event_icd10_dx_pro_archived (work_item_id, encounter_id);
CREATE INDEX event_icd10_dx_pro_ibfk_4_archived ON event_icd10_dx_pro_archived (dx_reason);
CREATE INDEX event_icd10_dx_pro_ibfk_5_archived ON event_icd10_dx_pro_archived (code);
CREATE INDEX facility_media_ibfk_2 ON facility_media (media_id);
CREATE INDEX facility_media_ibfk_1 ON facility_media (work_item_id);
CREATE INDEX idx_temp ON insurance_category (work_item_id, encounter_ins_1);
CREATE INDEX api_failed_reset_ibfk_2 ON api_failed_reset (user_id);
CREATE INDEX idx_code ON code_weight (code);
CREATE INDEX index2 ON code_weight (code);
CREATE INDEX em_fac_charge_ibfk_2 ON em_fac_charge (charge_master_id);
CREATE INDEX encounter_activities_ibfk_1 ON encounter_activities (encounter_id);
CREATE INDEX encounter_activities_ibfk_2 ON encounter_activities (new_status);
CREATE INDEX encounter_activities_ibfk_3 ON encounter_activities (work_item_id);
CREATE INDEX mirth_encounter_ibfk_2 ON mirth_encounter (status);
CREATE INDEX api_failed_login_ibfk_1 ON api_failed_login (user_id);
CREATE INDEX attending_provider_ibfk_2 ON attending_provider (provider_id);
CREATE INDEX coder_notes_ibfk_3 ON coder_notes (work_item_id, encounter_id);
CREATE INDEX coder_notes_ibfk_4 ON coder_notes (updated_userid);
CREATE INDEX idx_encid_time ON coder_notes (encounter_id, user_time_entered);
CREATE INDEX coder_notes_ibfk_2 ON coder_notes (userid);
CREATE INDEX encounter_id ON encounter_insurance_plan (encounter_id, plan_id);
CREATE INDEX event_pqrs_pro_ibfk_5 ON event_pqrs_pro (work_item_id, effective_year, pqrs_measure);
CREATE INDEX uniq_measure ON event_pqrs_pro (encounter_id, pqrs_measure, pqrs_num, effective_year);
CREATE INDEX fk_year_meas_code_mod ON event_pqrs_pro (effective_year, pqrs_measure, pqrs_num, modifier);
CREATE INDEX pqrs_num ON event_pqrs_pro (pqrs_num, effective_year, pqrs_measure);
CREATE INDEX pqrs_num_2 ON event_pqrs_pro (pqrs_num, modifier);
CREATE INDEX event_pqrs_pro_ibfk_3 ON event_pqrs_pro (userid);
CREATE INDEX event_pqrs_pro_ibfk_4 ON event_pqrs_pro (qa_userid);
CREATE INDEX pqrs_measure_wi_ibfk_2 ON pqrs_measure_wi (effective_year, pqrs_measure);
CREATE INDEX idx_code ON lu_cpt (cpt_code);
CREATE INDEX idx_cpt_status ON lu_cpt (cpt_code, status);
CREATE INDEX encounter_scribe_ibfk_2 ON encounter_scribe (user_id);
CREATE INDEX event_cpt_pro_ibfk_3 ON event_cpt_pro (mid_provider);
CREATE INDEX event_cpt_pro_ibfk_5 ON event_cpt_pro (userid);
CREATE INDEX event_cpt_pro_ibfk_6 ON event_cpt_pro (qa_userid);
CREATE INDEX work_item_id_2 ON event_cpt_pro (work_item_id, encounter_id);
CREATE INDEX idx_enc_id ON event_cpt_pro (encounter_id);
CREATE INDEX idx_procedure_num ON event_cpt_pro (procedure_num);
CREATE INDEX event_cpt_pro_ibfk_2 ON event_cpt_pro (procedure_provider);
CREATE INDEX encounter_id ON event_cpt_pro (encounter_id, procedure_num, procedure_qty, procedure_modifier1, procedure_modifier2, procedure_provider, mid_provider, qa_disagree, qa_procedure_num, qa_procedure_qty, qa_procedure_modifier, qa_procedure_provider, qa_reason, work_item_id, cosigning_physician, procedure_date);
CREATE INDEX fk_patient_status_id ON event_cpt_pro (patient_status_id);
CREATE INDEX event_cpt_pro_ibfk_13 ON event_cpt_pro (cosigning_physician);
CREATE INDEX event_cpt_pro_ibfk_4 ON event_cpt_pro (qa_procedure_provider);
CREATE INDEX user_access_ibfk_1 ON user_access (user_id);
CREATE INDEX user_access_ibfk_3 ON user_access (user_type_id);
CREATE INDEX work_item_id ON user_access (work_item_id, user_id, user_type_id);
CREATE INDEX level ON em_level (level);
CREATE INDEX encounter_details_ibfk_6 ON encounter_details (dataentry_assigned_tec);
CREATE INDEX idx_code_assigned ON encounter_details (code_assigned);
CREATE INDEX idx_composite ON encounter_details (qa_complete, code_assigned, encounter_id, work_item_id);
CREATE INDEX encounter_details_ibfk_4 ON encounter_details (qa_assigned);
CREATE INDEX encounter_details_ibfk_5 ON encounter_details (dataentry_assigned);
CREATE INDEX encounter_details_ibfk_7 ON encounter_details (dataentry_assigned_pf);
CREATE INDEX encounter_details_ibfk_8 ON encounter_details (encounter_status);
CREATE INDEX uniq_wi_enc ON encounter_details (work_item_id, encounter_id);
CREATE INDEX idx_enc_id ON encounter_details (encounter_id);
CREATE INDEX unique_wi_fac ON work_items (work_item_id, facility_id);
CREATE INDEX fk_work_item_1_idx ON work_items (alt_init_status);
CREATE INDEX idx_facility ON work_items (facility_id);
CREATE INDEX idx_related_wi_idx ON work_items (related_wi);
CREATE INDEX work_item_ibfk_2 ON work_items (work_item_type_id);
CREATE INDEX lu_pqrs_ibfk_1 ON lu_pqrs (effective_year, pqrs_measure);
CREATE INDEX pqrs_num ON lu_pqrs (pqrs_num, effective_year, pqrs_measure);
CREATE INDEX Full_Name ON client (full_name, short_name);
CREATE INDEX id_UNIQUE ON modifiers (id);
CREATE INDEX user_feature_ibfk_1 ON user_feature (feature_id);
CREATE INDEX event_dx_fac_ibfk_3 ON event_dx_fac (qa_userid);
CREATE INDEX event_dx_fac_ibfk_4 ON event_dx_fac (encounter_id);
CREATE INDEX event_dx_fac_ibfk_5 ON event_dx_fac (userid);
CREATE INDEX event_dx_fac_ibfk_6 ON event_dx_fac (work_item_id, encounter_id);
CREATE INDEX import_file_ibfk_2 ON import_file (work_item_id);
CREATE INDEX event_cdi_ibfk_2 ON event_cdi (userid);
CREATE INDEX event_cdi_ibfk_3 ON event_cdi (qa_userid);
CREATE INDEX event_cdi_ibfk_4 ON event_cdi (encounter_id);
CREATE INDEX event_cdi_ibfk_5 ON event_cdi (work_item_id);
CREATE INDEX assigned_provider_ibfk_1 ON assigned_provider (encounter_id);
CREATE INDEX charge_hold_cpt_ibfk_1 ON charge_hold_cpt (work_item_id, encounter_id);
CREATE INDEX fk_patient_status_id ON event_cpt_fac (patient_status_id);
CREATE INDEX idx_enc_id ON event_cpt_fac (encounter_id);
CREATE INDEX event_cpt_fac_ibfk_2 ON event_cpt_fac (procedure_provider);
CREATE INDEX event_cpt_fac_ibfk_6 ON event_cpt_fac (qa_userid);
CREATE INDEX event_cpt_fac_ibfk_4 ON event_cpt_fac (qa_procedure_provider);
CREATE INDEX event_cpt_fac_ibfk_5 ON event_cpt_fac (userid);
CREATE INDEX work_item_id_2 ON event_cpt_fac (work_item_id, encounter_id);
CREATE INDEX encounter_id ON event_cpt_fac (encounter_id, procedure_num, procedure_qty, procedure_modifier1, procedure_modifier2, procedure_provider, mid_provider, qa_disagree, qa_procedure_num, qa_procedure_qty, qa_procedure_modifier, qa_procedure_provider, qa_reason, work_item_id, cosigning_physician, procedure_date);
CREATE INDEX idx_procedure_num ON event_cpt_fac (procedure_num);
CREATE INDEX event_cpt_fac_ibfk_13 ON event_cpt_fac (cosigning_physician);
CREATE INDEX event_cpt_fac_ibfk_3 ON event_cpt_fac (mid_provider);
CREATE INDEX provider_ibfk_2 ON provider (added_by);
CREATE INDEX uniq_lov ON provider (provider_lov);
CREATE INDEX user_id ON provider (user_id);
CREATE INDEX idx_email ON provider (provider_email);
CREATE INDEX provider_ibfk_1 ON provider (type_id);
CREATE INDEX work_item_em_level_ibfk_2 ON work_item_em_level (charge_type);
CREATE INDEX event_hcpcs_fac_ibfk_5 ON event_hcpcs_fac (qa_userid);
CREATE INDEX event_hcpcs_fac_ibfk_7 ON event_hcpcs_fac (encounter_id);
CREATE INDEX event_hcpcs_fac_ibfk_9 ON event_hcpcs_fac (userid);
CREATE INDEX work_item_id_2 ON event_hcpcs_fac (work_item_id, encounter_id);
CREATE INDEX fk_patient_status_id ON event_hcpcs_fac (patient_status_id);
CREATE INDEX event_hcpcs_fac_ibfk_2 ON event_hcpcs_fac (hcpcs_provider);
CREATE INDEX event_hcpcs_fac_ibfk_3 ON event_hcpcs_fac (qa_procedure_provider);
CREATE INDEX user_pubkey_ibfk_1 ON user_pubkey (user_id);
CREATE INDEX FK_client_dx_encounter_id ON client_dx (encounter_id);
CREATE INDEX FK_client_dx_transaction_id ON client_dx (transaction_id);
CREATE INDEX FK_client_dx_work_item_id ON client_dx (work_item_id);
CREATE INDEX event_icd10_dx_pro_ibfk_2 ON event_icd10_dx_pro (qa_userid);
CREATE INDEX event_icd10_dx_pro_ibfk_3 ON event_icd10_dx_pro (work_item_id, encounter_id);
CREATE INDEX event_icd10_dx_pro_ibfk_4 ON event_icd10_dx_pro (dx_reason);
CREATE INDEX event_icd10_dx_pro_ibfk_5 ON event_icd10_dx_pro (code);
CREATE INDEX event_icd10_dx_pro_ibfk_1 ON event_icd10_dx_pro (userid);
CREATE INDEX event_special_fac_ibfk_9 ON event_special_fac (qa_userid);
CREATE INDEX event_special_fac_ibfk_11 ON event_special_fac (encounter_id);
CREATE INDEX event_special_fac_ibfk_4 ON event_special_fac (work_item_id, encounter_id);
CREATE INDEX event_special_fac_ibfk_5 ON event_special_fac (cosigning_physician);
CREATE INDEX event_special_fac_ibfk_6 ON event_special_fac (procedure_provider);
CREATE INDEX event_special_fac_ibfk_7 ON event_special_fac (qa_procedure_provider);
CREATE INDEX event_special_fac_ibfk_8 ON event_special_fac (userid);
CREATE INDEX fk_user_organization_1_idx ON user (user_organisation);
CREATE INDEX idx_email ON user (user_email_address);
CREATE INDEX user_ibfk_1 ON user (added_by);
CREATE INDEX uniq_email ON user (user_email_address);
CREATE INDEX event_dx_pro_ibfk_3 ON event_dx_pro (qa_userid);
CREATE INDEX event_dx_pro_ibfk_4 ON event_dx_pro (encounter_id);
CREATE INDEX event_dx_pro_ibfk_5 ON event_dx_pro (userid);
CREATE INDEX event_dx_pro_ibfk_6 ON event_dx_pro (work_item_id, encounter_id);
CREATE INDEX provider_type_ibfk_1 ON provider_types (category);
CREATE INDEX provider_id ON provider_wi (provider_id, work_item_id, responsible_user_id);
CREATE INDEX provider_wi_ibfk_3 ON provider_wi (responsible_user_id);
CREATE INDEX uniq_wi_client_ref ON provider_wi (work_item_id, client_ref);
CREATE INDEX work_item_report_ibfk_2 ON work_item_report (report_id);
CREATE INDEX type_validity_code ON lu_icd9 (CODE_TYPE, VALIDITY, ICD9_CODE);
CREATE INDEX type_validity_codenp ON lu_icd9 (CODE_TYPE, VALIDITY, code_no_periods);
CREATE INDEX idx_code_no_periods ON lu_icd9 (code_no_periods);
CREATE INDEX encounter_census_ibfk_1 ON encounter_census (work_item_id);
CREATE INDEX cpt_blacklist_ibfk_1 ON cpt_blacklist (work_item_id);
CREATE INDEX cpt_blacklist_ibfk_2 ON cpt_blacklist (charge_type);
CREATE INDEX cpt_code ON cpt_blacklist (cpt_code, charge_type, work_item_id);
CREATE INDEX reason_description ON suspend_master (reason_description, work_item_id);
CREATE INDEX suspend_master_ibfk_1 ON suspend_master (work_item_id);
CREATE INDEX user_id_fk ON user_password_reset (user_id);
CREATE INDEX suspend_events_ibfk_4 ON suspend_events (work_item_id, encounter_id);
CREATE INDEX fk_qa_user ON suspend_events (qa_userid);
CREATE INDEX fk_user ON suspend_events (userid);
CREATE INDEX idx_enc_id ON suspend_events (encounter_id);
CREATE INDEX suspend_events_ibfk_3 ON suspend_events (suspend_released_by);
CREATE INDEX work_item_em_level_ibfk_2 ON work_item_obs_codes (charge_type);
CREATE INDEX idx_client ON facilities (client_disposition_name);


CREATE INDEX fk_susp_cat ON suspend_reason (suspend_category);
CREATE INDEX idx_reason_desc ON suspend_reason (reason_description);
CREATE INDEX uniq_wi_reason ON suspend_reason (work_item_id, reason_description);
CREATE INDEX work_item_billing_provider_ibfk_1 ON work_item_billing_provider (provider_category);
CREATE INDEX encounter_hand_ibfk_1 ON encounter_hand (encounter_id);
CREATE INDEX unique_id ON notification_emails (id);
CREATE INDEX unique_id ON user_emails (id);
CREATE INDEX cpt_fac_charge_ibfk_2 ON cpt_fac_charge (charge_master_id);
CREATE INDEX event_icd10_dx_pro_ibfk_1_archived ON event_icd10_dx_pro_archived (user_id);
CREATE INDEX event_icd10_dx_pro_ibfk_2_archived ON event_icd10_dx_pro_archived (qa_user_id);
CREATE INDEX event_icd10_dx_pro_ibfk_3_archived ON event_icd10_dx_pro_archived (work_item_id, encounter_id);
CREATE INDEX event_icd10_dx_pro_ibfk_4_archived ON event_icd10_dx_pro_archived (dx_reason);
CREATE INDEX event_icd10_dx_pro_ibfk_5_archived ON event_icd10_dx_pro_archived (code);
CREATE INDEX event_icd10_dx_fac_ibfk_3_archived ON event_icd10_dx_fac_archived (work_item_id, encounter_id);
CREATE INDEX event_icd10_dx_fac_ibfk_4_archived ON event_icd10_dx_fac_archived (dx_reason);
CREATE INDEX event_icd10_dx_fac_ibfk_5_archived ON event_icd10_dx_fac_archived (code);
CREATE INDEX event_icd10_dx_fac_ibfk_1_archived ON event_icd10_dx_fac_archived (user_id);
CREATE INDEX event_icd10_dx_fac_ibfk_2_archived ON event_icd10_dx_fac_archived (qa_user_id);
CREATE INDEX coding_type ON cdi_reason (coding_type, cdi_area, cdi_item);
CREATE INDEX event_pqri_pro_ibfk_5 ON event_pqri_pro (user_id);
CREATE INDEX event_pqri_pro_ibfk_6 ON event_pqri_pro (encounter_id);
CREATE INDEX event_pqri_pro_ibfk_3 ON event_pqri_pro (qa_user_id);
CREATE INDEX change_password_ibfk_1 ON change_password (user_id);

*********** ERROR: relation "event_special_pro_ibfk_8" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_special_pro_ibfk_8 ON event_special_pro (user_id);
*********** ERROR: relation "facility_media" does not exist (SQLSTATE 42P01) ************

CREATE INDEX facility_media_ibfk_2 ON facility_media (media_id);

CREATE INDEX facility_media_ibfk_1 ON facility_media (work_item_id);
*********** ERROR: column "alt_init_status" does not exist (SQLSTATE 42703) ************

CREATE INDEX fk_work_item_1_idx ON work_items (alt_init_status);
*********** ERROR: relation "lu_icd9" does not exist (SQLSTATE 42P01) ************

CREATE INDEX type_validity_code ON lu_icd9 (CODE_TYPE, VALIDITY, ICD9_CODE);

CREATE INDEX type_validity_codenp ON lu_icd9 (CODE_TYPE, VALIDITY, code_no_periods);

CREATE INDEX idx_code_no_periods ON lu_icd9 (code_no_periods);
*********** ERROR: relation "suspend_master_ibfk_1" already exists (SQLSTATE 42P07) ************

CREATE INDEX suspend_master_ibfk_1 ON suspend_master (work_item_id);
*********** ERROR: relation "charge_master_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX charge_master_ibfk_2 ON charge_master (provider_category);
*********** ERROR: relation "idx_dos" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_dos ON encounters (encounter_dos);
*********** ERROR: relation "work_item_patient_status_ibfk_1" already exists (SQLSTATE 42P07) ************

CREATE INDEX work_item_patient_status_ibfk_1 ON work_item_patient_status (work_item_id);
*********** ERROR: relation "event_cpt_pro_ibfk_5" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_cpt_pro_ibfk_5 ON event_cpt_pro (user_id);
*********** ERROR: relation "event_cpt_pro_ibfk_6" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_cpt_pro_ibfk_6 ON event_cpt_pro (qa_user_id);
*********** ERROR: relation "event_icd10_dx_pro_ibfk_4" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_icd10_dx_pro_ibfk_4 ON event_icd10_dx_pro (dx_reason);
*********** ERROR: relation "event_pqrs_pro_ibfk_5" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_pqrs_pro_ibfk_5 ON event_pqrs_pro (work_item_id, effective_year, pqrs_measure);
*********** ERROR: relation "work_item_em_level_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX work_item_em_level_ibfk_2 ON work_item_obs_codes (charge_type);
*********** ERROR: relation "cpt_code" already exists (SQLSTATE 42P07) ************

CREATE INDEX cpt_code ON cpt_blacklist (cpt_code, charge_type, work_item_id);
*********** ERROR: relation "user_password_reset" does not exist (SQLSTATE 42P01) ************

CREATE INDEX user_id_fk ON user_password_reset (user_id);
*********** ERROR: relation "idx_wi_type_cpt_dt" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_wi_type_cpt_dt ON charge_master (work_item_id, charge_type, cpt_hcpcs_code, effective_date);
*********** ERROR: relation "uniq_wi_type_date_cpt_cat_internal" already exists (SQLSTATE 42P07) ************

CREATE INDEX uniq_wi_type_date_cpt_cat_internal ON charge_master (work_item_id, charge_type, effective_date, cpt_hcpcs_code, provider_category, client_internal_code);
*********** ERROR: relation "event_hcpcs_pro_ibfk_7" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_hcpcs_pro_ibfk_7 ON event_hcpcs_pro (encounter_id);
*********** ERROR: column "related_wi" does not exist (SQLSTATE 42703) ************

CREATE INDEX idx_related_wi_idx ON work_items (related_wi);
*********** ERROR: relation "import_file" does not exist (SQLSTATE 42P01) ************

CREATE INDEX import_file_ibfk_2 ON import_file (work_item_id);
*********** ERROR: relation "user_pubkey" does not exist (SQLSTATE 42P01) ************

CREATE INDEX user_pubkey_ibfk_1 ON user_pubkey (user_id);
*********** ERROR: relation "fk_user" already exists (SQLSTATE 42P07) ************

CREATE INDEX fk_user ON suspend_events (user_id);
*********** ERROR: relation "work_item_em_level" does not exist (SQLSTATE 42P01) ************

CREATE INDEX work_item_em_level_ibfk_2 ON work_item_em_level (charge_type);
*********** ERROR: relation "event_hcpcs_fac_ibfk_5" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_hcpcs_fac_ibfk_5 ON event_hcpcs_fac (qa_user_id);
*********** ERROR: relation "charge_master_ibfk_1" already exists (SQLSTATE 42P07) ************

CREATE INDEX charge_master_ibfk_1 ON charge_master (charge_type);
*********** ERROR: relation "idx_enc_id" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_enc_id ON event_em_fac (encounter_id);

CREATE INDEX idx_enc_id ON event_em_pro (encounter_id);

CREATE INDEX idx_enc_id ON event_cpt_pro (encounter_id);

CREATE INDEX idx_enc_id ON encounter_details (encounter_id);

CREATE INDEX idx_enc_id ON event_cpt_fac (encounter_id);

CREATE INDEX idx_enc_id ON suspend_events (encounter_id);
*********** ERROR: relation "encounter_ibfk_1" already exists (SQLSTATE 42P07) ************

CREATE INDEX encounter_ibfk_1 ON encounters (attend_provider_id);
*********** ERROR: relation "event_icd10_dx_fac_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_icd10_dx_fac_ibfk_3 ON event_icd10_dx_fac (work_item_id, encounter_id);
*********** ERROR: relation "encounter_activities_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX encounter_activities_ibfk_3 ON encounter_activities (work_item_id);
*********** ERROR: relation "client" does not exist (SQLSTATE 42P01) ************

CREATE INDEX Full_Name ON client (full_name, short_name);
*********** ERROR: relation "em_fac_charge" does not exist (SQLSTATE 42P01) ************

CREATE INDEX em_fac_charge_ibfk_2 ON em_fac_charge (charge_master_id);
*********** ERROR: relation "encounter_activities_ibfk_1" already exists (SQLSTATE 42P07) ************

CREATE INDEX encounter_activities_ibfk_1 ON encounter_activities (encounter_id);
*********** ERROR: relation "encounter_insurance_plan" does not exist (SQLSTATE 42P01) ************

CREATE INDEX encounter_id ON encounter_insurance_plan (encounter_id, plan_id);
*********** ERROR: relation "event_hcpcs_fac_ibfk_7" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_hcpcs_fac_ibfk_7 ON event_hcpcs_fac (encounter_id);
*********** ERROR: relation "event_icd10_dx_fac_ibfk_4" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_icd10_dx_fac_ibfk_4 ON event_icd10_dx_fac (dx_reason);
*********** ERROR: relation "event_special_fac_ibfk_4" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_special_fac_ibfk_4 ON event_special_fac (work_item_id, encounter_id);
*********** ERROR: relation "event_dx_pro_ibfk_6" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_dx_pro_ibfk_6 ON event_dx_pro (work_item_id, encounter_id);
*********** ERROR: relation "provider_type_ibfk_1" already exists (SQLSTATE 42P07) ************

CREATE INDEX provider_type_ibfk_1 ON provider_types (category);
*********** ERROR: relation "idx_code" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_code ON icd10 (code);

CREATE INDEX idx_code ON lu_cpt (cpt_code);
*********** ERROR: relation "idx_header_tbl" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_header_tbl ON icd10 (header, tbl);
*********** ERROR: relation "event_cpt_fac_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_cpt_fac_ibfk_2 ON event_cpt_fac (procedure_provider);
*********** ERROR: relation "uniq_wi_client_ref" already exists (SQLSTATE 42P07) ************

CREATE INDEX uniq_wi_client_ref ON provider_wi (work_item_id, client_ref);
*********** ERROR: relation "work_item_report" does not exist (SQLSTATE 42P01) ************

CREATE INDEX work_item_report_ibfk_2 ON work_item_report (report_id);
*********** ERROR: relation "event_em_fac_ibfk_6" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_em_fac_ibfk_6 ON event_em_fac (qa_user_id);
*********** ERROR: relation "idx_fin" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_fin ON encounters (patient_fin);
*********** ERROR: relation "event_cdi_pro_ibfk_4" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_cdi_pro_ibfk_4 ON event_cdi_pro (encounter_id);
*********** ERROR: relation "event_cpt_fac_ibfk_5" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_cpt_fac_ibfk_5 ON event_cpt_fac (user_id);
*********** ERROR: relation "suspend_events_ibfk_4" already exists (SQLSTATE 42P07) ************

CREATE INDEX suspend_events_ibfk_4 ON suspend_events (work_item_id, encounter_id);
*********** ERROR: relation "event_icd10_dx_fac_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_icd10_dx_fac_ibfk_2 ON event_icd10_dx_fac (qa_user_id);
*********** ERROR: relation "event_dx_fac_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_dx_fac_ibfk_3 ON event_dx_fac (qa_user_id);
*********** ERROR: column "id" does not exist (SQLSTATE 42703) ************

CREATE INDEX id_UNIQUE ON modifiers (id);
*********** ERROR: relation "provider_wi_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX provider_wi_ibfk_3 ON provider_wi (responsible_user_id);
*********** ERROR: relation "wi_pt_ft_hcpcs_date" already exists (SQLSTATE 42P07) ************

CREATE INDEX wi_pt_ft_hcpcs_date ON charge_master (work_item_id, profee_type, facility_type, cpt_hcpcs_code, effective_date);
*********** ERROR: relation "encounter_history_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX encounter_history_ibfk_2 ON encounter_history (user_id);
*********** ERROR: relation "event_hcpcs_pro_ibfk_5" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_hcpcs_pro_ibfk_5 ON event_hcpcs_pro (qa_user_id);
*********** ERROR: relation "event_em_pro_ibfk_5" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_em_pro_ibfk_5 ON event_em_pro (user_id);
*********** ERROR: relation "api_failed_login" does not exist (SQLSTATE 42P01) ************

CREATE INDEX api_failed_login_ibfk_1 ON api_failed_login (user_id);
*********** ERROR: relation "event_pqrs_pro_ibfk_4" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_pqrs_pro_ibfk_4 ON event_pqrs_pro (qa_user_id);
*********** ERROR: relation "event_special_fac_ibfk_6" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_special_fac_ibfk_6 ON event_special_fac (procedure_provider);
*********** ERROR: relation "event_special_fac_ibfk_7" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_special_fac_ibfk_7 ON event_special_fac (qa_procedure_provider);
*********** ERROR: relation "event_em_fac_ibfk_4" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_em_fac_ibfk_4 ON event_em_fac (qa_procedure_provider);
*********** ERROR: relation "event_hcpcs_pro_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_hcpcs_pro_ibfk_2 ON event_hcpcs_pro (hcpcs_provider);
*********** ERROR: relation "encounter_scribe" does not exist (SQLSTATE 42P01) ************

CREATE INDEX encounter_scribe_ibfk_2 ON encounter_scribe (user_id);
*********** ERROR: relation "idx_facility" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_facility ON work_items (facility_id);
*********** ERROR: relation "work_item_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX work_item_ibfk_2 ON work_items (work_item_type_id);
*********** ERROR: relation "client_dx" does not exist (SQLSTATE 42P01) ************

CREATE INDEX FK_client_dx_encounter_id ON client_dx (encounter_id);

CREATE INDEX FK_client_dx_transaction_id ON client_dx (transaction_id);

CREATE INDEX FK_client_dx_work_item_id ON client_dx (work_item_id);
*********** ERROR: relation "encounter_census" does not exist (SQLSTATE 42P01) ************

CREATE INDEX encounter_census_ibfk_1 ON encounter_census (work_item_id);
*********** ERROR: relation "event_em_pro_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_em_pro_ibfk_3 ON event_em_pro (em_midlevel);
*********** ERROR: relation "event_icd10_dx_fac_ibfk_5" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_icd10_dx_fac_ibfk_5 ON event_icd10_dx_fac (code);
*********** ERROR: relation "event_dx_fac_ibfk_5" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_dx_fac_ibfk_5 ON event_dx_fac (user_id);
*********** ERROR: relation "insurance_category" does not exist (SQLSTATE 42P01) ************

CREATE INDEX idx_temp ON insurance_category (work_item_id, encounter_ins_1);
*********** ERROR: relation "encounter_details_ibfk_6" already exists (SQLSTATE 42P07) ************

CREATE INDEX encounter_details_ibfk_6 ON encounter_details (dataentry_assigned_tec);
*********** ERROR: relation "idx_code_assigned" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_code_assigned ON encounter_details (code_assigned);
*********** ERROR: relation "cpt_blacklist_ibfk_1" already exists (SQLSTATE 42P07) ************

CREATE INDEX cpt_blacklist_ibfk_1 ON cpt_blacklist (work_item_id);
*********** ERROR: relation "em_level" already exists (SQLSTATE 42P07) ************

CREATE INDEX em_level ON event_em_fac (em_level);

CREATE INDEX em_level ON event_em_pro (em_level);
*********** ERROR: relation "idx_mrn_temp" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_mrn_temp ON encounters (patient_mrn);
*********** ERROR: relation "event_hcpcs_pro_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_hcpcs_pro_ibfk_3 ON event_hcpcs_pro (qa_procedure_provider);
*********** ERROR: relation "coder_notes_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX coder_notes_ibfk_2 ON coder_notes (user_id);
*********** ERROR: relation "idx_composite" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_composite ON encounter_details (qa_complete, code_assigned, encounter_id, work_item_id);
*********** ERROR: relation "event_hcpcs_fac_ibfk_9" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_hcpcs_fac_ibfk_9 ON event_hcpcs_fac (user_id);
*********** ERROR: relation "encounter_id" already exists (SQLSTATE 42P07) ************

CREATE INDEX encounter_id ON event_em_fac (encounter_id, work_item_id, event_dos);

CREATE INDEX encounter_id ON event_em_pro (encounter_id, work_item_id, event_dos);

CREATE INDEX encounter_id ON event_cpt_pro (encounter_id, procedure_num, procedure_qty, procedure_modifier1, procedure_modifier2, procedure_provider, mid_provider, qa_disagree, qa_procedure_num, qa_procedure_qty, qa_procedure_modifier, qa_procedure_provider, qa_reason, work_item_id, cosigning_physician, procedure_date);

CREATE INDEX encounter_id ON event_cpt_fac (encounter_id, procedure_num, procedure_qty, procedure_modifier1, procedure_modifier2, procedure_provider, mid_provider, qa_disagree, qa_procedure_num, qa_procedure_qty, qa_procedure_modifier, qa_procedure_provider, qa_reason, work_item_id, cosigning_physician, procedure_date);
*********** ERROR: relation "mirth_exception_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX mirth_exception_ibfk_2 ON mirth_exception (event_em_fac_id);
*********** ERROR: relation "code_weight" does not exist (SQLSTATE 42P01) ************

CREATE INDEX idx_code ON code_weight (code);

CREATE INDEX index2 ON code_weight (code);
*********** ERROR: relation "idx_cpt_status" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_cpt_status ON lu_cpt (cpt_code, status);
*********** ERROR: relation "cpt_blacklist_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX cpt_blacklist_ibfk_2 ON cpt_blacklist (charge_type);
*********** ERROR: relation "event_special_pro_ibfk_4" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_special_pro_ibfk_4 ON event_special_pro (work_item_id, encounter_id);
*********** ERROR: relation "coder_notes_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX coder_notes_ibfk_3 ON coder_notes (work_item_id, encounter_id);
*********** ERROR: relation "level" already exists (SQLSTATE 42P07) ************

CREATE INDEX level ON em_level (level);
*********** success ************

;
*********** ERROR: relation "event_special_fac_ibfk_11" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_special_fac_ibfk_11 ON event_special_fac (encounter_id);
*********** ERROR: relation "mirth_exception_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX mirth_exception_ibfk_3 ON mirth_exception (event_cpt_fac_id);
*********** ERROR: relation "idx_ins" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_ins ON encounters (encounter_primary_type);
*********** ERROR: relation "api_failed_reset" does not exist (SQLSTATE 42P01) ************

CREATE INDEX api_failed_reset_ibfk_2 ON api_failed_reset (user_id);
*********** ERROR: relation "idx_encid_time" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_encid_time ON coder_notes (encounter_id, user_time_entered);
*********** ERROR: relation "event_hcpcs_fac_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_hcpcs_fac_ibfk_3 ON event_hcpcs_fac (qa_procedure_provider);
*********** ERROR: relation "event_icd10_dx_pro_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_icd10_dx_pro_ibfk_2 ON event_icd10_dx_pro (qa_user_id);
*********** ERROR: relation "event_cdi" does not exist (SQLSTATE 42P01) ************

CREATE INDEX event_cdi_ibfk_2 ON event_cdi (user_id);

CREATE INDEX event_cdi_ibfk_3 ON event_cdi (qa_user_id);

CREATE INDEX event_cdi_ibfk_4 ON event_cdi (encounter_id);

CREATE INDEX event_cdi_ibfk_5 ON event_cdi (work_item_id);
*********** ERROR: relation "event_cpt_fac_ibfk_13" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_cpt_fac_ibfk_13 ON event_cpt_fac (cosigning_physician);
*********** ERROR: relation "event_em_fac_ibfk_15" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_em_fac_ibfk_15 ON event_em_fac (cosigning_physician);
*********** ERROR: relation "event_cdi_pro_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_cdi_pro_ibfk_2 ON event_cdi_pro (user_id);
*********** ERROR: relation "event_em_pro_ibfk_15" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_em_pro_ibfk_15 ON event_em_pro (cosigning_physician);
*********** ERROR: relation "attending_provider" does not exist (SQLSTATE 42P01) ************

CREATE INDEX attending_provider_ibfk_2 ON attending_provider (provider_id);
*********** ERROR: relation "pqrs_measure_wi" does not exist (SQLSTATE 42P01) ************

CREATE INDEX pqrs_measure_wi_ibfk_2 ON pqrs_measure_wi (effective_year, pqrs_measure);
*********** ERROR: relation "event_cpt_pro_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_cpt_pro_ibfk_3 ON event_cpt_pro (mid_provider);
*********** ERROR: relation "event_dx_pro_ibfk_4" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_dx_pro_ibfk_4 ON event_dx_pro (encounter_id);
*********** ERROR: relation "fk_qa_user" already exists (SQLSTATE 42P07) ************

CREATE INDEX fk_qa_user ON suspend_events (qa_user_id);
*********** ERROR: relation "event_em_fac_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_em_fac_ibfk_3 ON event_em_fac (em_midlevel);
*********** ERROR: relation "idx_encid" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_encid ON encounter_history (encounter_id);
*********** ERROR: relation "event_cdi_pro_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_cdi_pro_ibfk_3 ON event_cdi_pro (qa_user_id);
*********** ERROR: relation "fk_year_meas_code_mod" already exists (SQLSTATE 42P07) ************

CREATE INDEX fk_year_meas_code_mod ON event_pqrs_pro (effective_year, pqrs_measure, pqrs_num, modifier);
*********** ERROR: relation "event_cpt_pro_ibfk_4" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_cpt_pro_ibfk_4 ON event_cpt_pro (qa_procedure_provider);
*********** ERROR: relation "provider_id" already exists (SQLSTATE 42P07) ************

CREATE INDEX provider_id ON provider_wi (provider_id, work_item_id, responsible_user_id);
*********** ERROR: relation "idx_with_period" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_with_period ON icd10 (effective_date, tbl, header, code_with_period);
*********** ERROR: relation "reason_description" already exists (SQLSTATE 42P07) ************

CREATE INDEX reason_description ON suspend_master (reason_description, work_item_id);
*********** ERROR: relation "event_special_fac_ibfk_9" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_special_fac_ibfk_9 ON event_special_fac (qa_user_id);
*********** ERROR: relation "event_dx_pro_ibfk_5" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_dx_pro_ibfk_5 ON event_dx_pro (user_id);
*********** ERROR: relation "idx_charge_type" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_charge_type ON charge_master (effective_date, charge_type);
*********** ERROR: relation "idx_wi_type_dt_cpt" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_wi_type_dt_cpt ON charge_master (work_item_id, charge_type, effective_date, cpt_hcpcs_code);
*********** ERROR: relation "mdm_total" already exists (SQLSTATE 42P07) ************

CREATE INDEX mdm_total ON event_em_fac (mdm_total);

CREATE INDEX mdm_total ON event_em_pro (mdm_total);
*********** ERROR: relation "effective_date" already exists (SQLSTATE 42P07) ************

CREATE INDEX effective_date ON icd10 (effective_date, tbl, header, code);
*********** ERROR: relation "event_cdi_pro_ibfk_5" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_cdi_pro_ibfk_5 ON event_cdi_pro (work_item_id);
*********** ERROR: relation "uniq_measure" already exists (SQLSTATE 42P07) ************

CREATE INDEX uniq_measure ON event_pqrs_pro (encounter_id, pqrs_measure, pqrs_num, effective_year);
*********** ERROR: relation "event_icd10_dx_fac_ibfk_1" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_icd10_dx_fac_ibfk_1 ON event_icd10_dx_fac (user_id);
*********** ERROR: column "updated_user_id" does not exist (SQLSTATE 42703) ************

CREATE INDEX coder_notes_ibfk_4 ON coder_notes (updated_user_id);
*********** ERROR: relation "event_em_fac_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_em_fac_ibfk_2 ON event_em_fac (em_provider);
*********** ERROR: relation "event_special_pro_ibfk_7" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_special_pro_ibfk_7 ON event_special_pro (qa_procedure_provider);
*********** ERROR: relation "mirth_exception_ibfk_1" already exists (SQLSTATE 42P07) ************

CREATE INDEX mirth_exception_ibfk_1 ON mirth_exception (work_item_id, encounter_id);
*********** ERROR: relation "idx_name" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_name ON encounters (patient_name);
*********** ERROR: relation "event_hcpcs_pro_ibfk_9" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_hcpcs_pro_ibfk_9 ON event_hcpcs_pro (user_id);
*********** ERROR: relation "event_em_pro_ibfk_4" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_em_pro_ibfk_4 ON event_em_pro (qa_procedure_provider);
*********** ERROR: relation "event_cpt_pro_ibfk_13" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_cpt_pro_ibfk_13 ON event_cpt_pro (cosigning_physician);
*********** ERROR: relation "encounter_details_ibfk_7" already exists (SQLSTATE 42P07) ************

CREATE INDEX encounter_details_ibfk_7 ON encounter_details (dataentry_assigned_pf);
*********** ERROR: relation "event_icd10_dx_pro_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_icd10_dx_pro_ibfk_3 ON event_icd10_dx_pro (work_item_id, encounter_id);
*********** ERROR: relation "event_em_pro_ibfk_6" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_em_pro_ibfk_6 ON event_em_pro (qa_user_id);
*********** ERROR: relation "encounter_activities_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX encounter_activities_ibfk_2 ON encounter_activities (new_status);
*********** ERROR: relation "encounter_details_ibfk_4" already exists (SQLSTATE 42P07) ************

CREATE INDEX encounter_details_ibfk_4 ON encounter_details (qa_assigned);
*********** ERROR: relation "event_cpt_fac_ibfk_6" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_cpt_fac_ibfk_6 ON event_cpt_fac (qa_user_id);
*********** ERROR: relation "event_cpt_pro_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_cpt_pro_ibfk_2 ON event_cpt_pro (procedure_provider);
*********** ERROR: relation "event_icd10_dx_pro_ibfk_1" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_icd10_dx_pro_ibfk_1 ON event_icd10_dx_pro (user_id);
*********** ERROR: relation "event_special_fac_ibfk_8" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_special_fac_ibfk_8 ON event_special_fac (user_id);
*********** ERROR: relation "event_dx_pro_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_dx_pro_ibfk_3 ON event_dx_pro (qa_user_id);
*********** ERROR: relation "idx_client" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_client ON facilities (client_disposition_name);
*********** ERROR: relation "encounter_details_ibfk_8" already exists (SQLSTATE 42P07) ************

CREATE INDEX encounter_details_ibfk_8 ON encounter_details (encounter_status);
*********** ERROR: relation "event_icd10_dx_pro_ibfk_5" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_icd10_dx_pro_ibfk_5 ON event_icd10_dx_pro (code);
*********** ERROR: relation "fk_patient_status_id" already exists (SQLSTATE 42P07) ************

CREATE INDEX fk_patient_status_id ON charge_master (patient_status_id);

CREATE INDEX fk_patient_status_id ON event_em_fac (patient_status_id);

CREATE INDEX fk_patient_status_id ON event_hcpcs_pro (patient_status_id);

CREATE INDEX fk_patient_status_id ON event_em_pro (patient_status_id);

CREATE INDEX fk_patient_status_id ON event_cpt_pro (patient_status_id);

CREATE INDEX fk_patient_status_id ON event_cpt_fac (patient_status_id);

CREATE INDEX fk_patient_status_id ON event_hcpcs_fac (patient_status_id);
*********** ERROR: relation "event_em_fac_ibfk_5" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_em_fac_ibfk_5 ON event_em_fac (user_id);
*********** ERROR: relation "event_special_pro_ibfk_6" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_special_pro_ibfk_6 ON event_special_pro (procedure_provider);
*********** ERROR: relation "event_em_pro_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_em_pro_ibfk_2 ON event_em_pro (em_provider);
*********** ERROR: relation "pqrs_num" already exists (SQLSTATE 42P07) ************

CREATE INDEX pqrs_num ON event_pqrs_pro (pqrs_num, effective_year, pqrs_measure);
*********** ERROR: relation "event_pqrs_pro_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_pqrs_pro_ibfk_3 ON event_pqrs_pro (user_id);
*********** ERROR: relation "uniq_wi_enc" already exists (SQLSTATE 42P07) ************

CREATE INDEX uniq_wi_enc ON encounter_details (work_item_id, encounter_id);
*********** ERROR: relation "event_cpt_fac_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_cpt_fac_ibfk_3 ON event_cpt_fac (mid_provider);
*********** ERROR: relation "unique_wi_fac" already exists (SQLSTATE 42P07) ************

CREATE INDEX unique_wi_fac ON work_items (work_item_id, facility_id);
*********** ERROR: relation "assigned_provider" does not exist (SQLSTATE 42P01) ************

CREATE INDEX assigned_provider_ibfk_1 ON assigned_provider (encounter_id);
*********** ERROR: relation "idx_wi_eff_date" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_wi_eff_date ON charge_master (work_item_id, effective_date);
*********** ERROR: relation "event_special_pro_ibfk_9" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_special_pro_ibfk_9 ON event_special_pro (qa_user_id);
*********** ERROR: relation "idx_prov" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_prov ON encounters (encounter_assign_provider1);
*********** ERROR: relation "encounter_history_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX encounter_history_ibfk_3 ON encounter_history (work_item_id, encounter_id);
*********** ERROR: relation "event_type" does not exist (SQLSTATE 42P01) ************

CREATE INDEX Event ON event_type (event);
*********** ERROR: relation "pqrs_num_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX pqrs_num_2 ON event_pqrs_pro (pqrs_num, modifier);
*********** ERROR: relation "charge_hold_cpt" does not exist (SQLSTATE 42P01) ************

CREATE INDEX charge_hold_cpt_ibfk_1 ON charge_hold_cpt (work_item_id, encounter_id);
*********** ERROR: relation "event_hcpcs_fac_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_hcpcs_fac_ibfk_2 ON event_hcpcs_fac (hcpcs_provider);
*********** ERROR: relation "provider" does not exist (SQLSTATE 42P01) ************

CREATE INDEX provider_ibfk_2 ON provider (added_by);

CREATE INDEX uniq_lov ON provider (provider_lov);

CREATE INDEX user_id ON provider (user_id);

CREATE INDEX idx_email ON provider (provider_email);

CREATE INDEX provider_ibfk_1 ON provider (type_id);
*********** ERROR: relation "work_item_id_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX work_item_id_2 ON event_em_fac (work_item_id, encounter_id);

CREATE INDEX work_item_id_2 ON event_hcpcs_pro (work_item_id, encounter_id);

CREATE INDEX work_item_id_2 ON event_em_pro (work_item_id, encounter_id);

CREATE INDEX work_item_id_2 ON event_cpt_pro (work_item_id, encounter_id);

CREATE INDEX work_item_id_2 ON event_cpt_fac (work_item_id, encounter_id);

CREATE INDEX work_item_id_2 ON event_hcpcs_fac (work_item_id, encounter_id);
*********** ERROR: relation "idx_procedure_num" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_procedure_num ON event_cpt_pro (procedure_num);

CREATE INDEX idx_procedure_num ON event_cpt_fac (procedure_num);
*********** ERROR: relation "encounter_details_ibfk_5" already exists (SQLSTATE 42P07) ************

CREATE INDEX encounter_details_ibfk_5 ON encounter_details (dataentry_assigned);
*********** ERROR: relation "lu_pqrs" does not exist (SQLSTATE 42P01) ************

CREATE INDEX lu_pqrs_ibfk_1 ON lu_pqrs (effective_year, pqrs_measure);

CREATE INDEX pqrs_num ON lu_pqrs (pqrs_num, effective_year, pqrs_measure);
*********** ERROR: relation "event_dx_fac_ibfk_4" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_dx_fac_ibfk_4 ON event_dx_fac (encounter_id);
*********** ERROR: relation "event_dx_fac_ibfk_6" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_dx_fac_ibfk_6 ON event_dx_fac (work_item_id, encounter_id);
*********** ERROR: relation "event_special_pro_ibfk_5" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_special_pro_ibfk_5 ON event_special_pro (cosigning_physician);
*********** ERROR: relation "user_access" does not exist (SQLSTATE 42P01) ************

CREATE INDEX user_access_ibfk_1 ON user_access (user_id);

CREATE INDEX user_access_ibfk_3 ON user_access (user_type_id);

CREATE INDEX work_item_id ON user_access (work_item_id, user_id, user_type_id);
*********** ERROR: relation "user_feature" does not exist (SQLSTATE 42P01) ************

CREATE INDEX user_feature_ibfk_1 ON user_feature (feature_id);
*********** ERROR: relation "suspend_events_ibfk_3" already exists (SQLSTATE 42P07) ************

CREATE INDEX suspend_events_ibfk_3 ON suspend_events (suspend_released_by);
*********** ERROR: relation "idx_report_idx" already exists (SQLSTATE 42P07) ************

CREATE INDEX idx_report_idx ON charge_master (report);
*********** ERROR: relation "event_special_pro_ibfk_11" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_special_pro_ibfk_11 ON event_special_pro (encounter_id);
*********** ERROR: relation "mirth_encounter_ibfk_2" already exists (SQLSTATE 42P07) ************

CREATE INDEX mirth_encounter_ibfk_2 ON mirth_encounter (status);
*********** ERROR: relation "event_cpt_fac_ibfk_4" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_cpt_fac_ibfk_4 ON event_cpt_fac (qa_procedure_provider);
*********** ERROR: relation "event_special_fac_ibfk_5" already exists (SQLSTATE 42P07) ************

CREATE INDEX event_special_fac_ibfk_5 ON event_special_fac (cosigning_physician);
