package main

import (
	"fmt"
	"github.com/oarkflow/squealx/drivers/postgres"
	"log"
	"os"
	"strings"
)

var queries = `
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
CREATE INDEX event_em_fac_ibfk_5 ON event_em_fac (user_id);
CREATE INDEX event_em_fac_ibfk_6 ON event_em_fac (qa_user_id);
CREATE INDEX event_special_pro_ibfk_7 ON event_special_pro (qa_procedure_provider);
CREATE INDEX event_special_pro_ibfk_8 ON event_special_pro (user_id);
CREATE INDEX event_special_pro_ibfk_9 ON event_special_pro (qa_user_id);
CREATE INDEX event_special_pro_ibfk_11 ON event_special_pro (encounter_id);
CREATE INDEX event_special_pro_ibfk_4 ON event_special_pro (work_item_id, encounter_id);
CREATE INDEX event_special_pro_ibfk_5 ON event_special_pro (cosigning_physician);
CREATE INDEX event_special_pro_ibfk_6 ON event_special_pro (procedure_provider);
CREATE INDEX mirth_exception_ibfk_1 ON mirth_exception (work_item_id, encounter_id);
CREATE INDEX mirth_exception_ibfk_2 ON mirth_exception (event_em_fac_id);
CREATE INDEX mirth_exception_ibfk_3 ON mirth_exception (event_cpt_fac_id);
CREATE INDEX idx_mrn_temp ON encounters (patient_mrn);
CREATE INDEX encounter_ibfk_1 ON encounters (attend_provider_id);
CREATE INDEX idx_dos ON encounters (encounter_dos);
CREATE INDEX idx_fin ON encounters (patient_fin);
CREATE INDEX idx_ins ON encounters (encounter_primary_type);
CREATE INDEX idx_name ON encounters (patient_name);
CREATE INDEX idx_prov ON encounters (encounter_assign_provider1);
CREATE INDEX idx_encid ON encounter_history (encounter_id);
CREATE INDEX encounter_history_ibfk_2 ON encounter_history (user_id);
CREATE INDEX encounter_history_ibfk_3 ON encounter_history (work_item_id, encounter_id);
CREATE INDEX effective_date ON icd10 (effective_date, tbl, header, code);
CREATE INDEX idx_code ON icd10 (code);
CREATE INDEX idx_header_tbl ON icd10 (header, tbl);
CREATE INDEX idx_with_period ON icd10 (effective_date, tbl, header, code_with_period);
CREATE INDEX event_hcpcs_pro_ibfk_2 ON event_hcpcs_pro (hcpcs_provider);
CREATE INDEX event_hcpcs_pro_ibfk_3 ON event_hcpcs_pro (qa_procedure_provider);
CREATE INDEX event_hcpcs_pro_ibfk_5 ON event_hcpcs_pro (qa_user_id);
CREATE INDEX event_hcpcs_pro_ibfk_7 ON event_hcpcs_pro (encounter_id);
CREATE INDEX event_hcpcs_pro_ibfk_9 ON event_hcpcs_pro (user_id);
CREATE INDEX work_item_id_2 ON event_hcpcs_pro (work_item_id, encounter_id);
CREATE INDEX fk_patient_status_id ON event_hcpcs_pro (patient_status_id);
CREATE INDEX Event ON event_type (event);
CREATE INDEX work_item_patient_status_ibfk_1 ON work_item_patient_status (work_item_id);
CREATE INDEX event_cdi_pro_ibfk_2 ON event_cdi_pro (user_id);
CREATE INDEX event_cdi_pro_ibfk_3 ON event_cdi_pro (qa_user_id);
CREATE INDEX event_cdi_pro_ibfk_4 ON event_cdi_pro (encounter_id);
CREATE INDEX event_cdi_pro_ibfk_5 ON event_cdi_pro (work_item_id);
CREATE INDEX fk_patient_status_id ON event_em_pro (patient_status_id);
CREATE INDEX idx_enc_id ON event_em_pro (encounter_id);
CREATE INDEX event_em_pro_ibfk_2 ON event_em_pro (em_provider);
CREATE INDEX event_em_pro_ibfk_3 ON event_em_pro (em_midlevel);
CREATE INDEX event_em_pro_ibfk_5 ON event_em_pro (user_id);
CREATE INDEX work_item_id_2 ON event_em_pro (work_item_id, encounter_id);
CREATE INDEX em_level ON event_em_pro (em_level);
CREATE INDEX encounter_id ON event_em_pro (encounter_id, work_item_id, event_dos);
CREATE INDEX mdm_total ON event_em_pro (mdm_total);
CREATE INDEX event_em_pro_ibfk_15 ON event_em_pro (cosigning_physician);
CREATE INDEX event_em_pro_ibfk_4 ON event_em_pro (qa_procedure_provider);
CREATE INDEX event_em_pro_ibfk_6 ON event_em_pro (qa_user_id);
CREATE INDEX event_icd10_dx_fac_ibfk_1 ON event_icd10_dx_fac (user_id);
CREATE INDEX event_icd10_dx_fac_ibfk_2 ON event_icd10_dx_fac (qa_user_id);
CREATE INDEX event_icd10_dx_fac_ibfk_3 ON event_icd10_dx_fac (work_item_id, encounter_id);
CREATE INDEX event_icd10_dx_fac_ibfk_4 ON event_icd10_dx_fac (dx_reason);
CREATE INDEX event_icd10_dx_fac_ibfk_5 ON event_icd10_dx_fac (code);
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
CREATE INDEX coder_notes_ibfk_4 ON coder_notes (updated_user_id);
CREATE INDEX idx_encid_time ON coder_notes (encounter_id, user_time_entered);
CREATE INDEX coder_notes_ibfk_2 ON coder_notes (user_id);
CREATE INDEX encounter_id ON encounter_insurance_plan (encounter_id, plan_id);
CREATE INDEX event_pqrs_pro_ibfk_5 ON event_pqrs_pro (work_item_id, effective_year, pqrs_measure);
CREATE INDEX uniq_measure ON event_pqrs_pro (encounter_id, pqrs_measure, pqrs_num, effective_year);
CREATE INDEX fk_year_meas_code_mod ON event_pqrs_pro (effective_year, pqrs_measure, pqrs_num, modifier);
CREATE INDEX pqrs_num ON event_pqrs_pro (pqrs_num, effective_year, pqrs_measure);
CREATE INDEX pqrs_num_2 ON event_pqrs_pro (pqrs_num, modifier);
CREATE INDEX event_pqrs_pro_ibfk_3 ON event_pqrs_pro (user_id);
CREATE INDEX event_pqrs_pro_ibfk_4 ON event_pqrs_pro (qa_user_id);
CREATE INDEX pqrs_measure_wi_ibfk_2 ON pqrs_measure_wi (effective_year, pqrs_measure);
CREATE INDEX idx_code ON lu_cpt (cpt_code);
CREATE INDEX idx_cpt_status ON lu_cpt (cpt_code, status);
CREATE INDEX encounter_scribe_ibfk_2 ON encounter_scribe (user_id);
CREATE INDEX event_cpt_pro_ibfk_3 ON event_cpt_pro (mid_provider);
CREATE INDEX event_cpt_pro_ibfk_5 ON event_cpt_pro (user_id);
CREATE INDEX event_cpt_pro_ibfk_6 ON event_cpt_pro (qa_user_id);
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
CREATE INDEX event_dx_fac_ibfk_3 ON event_dx_fac (qa_user_id);
CREATE INDEX event_dx_fac_ibfk_4 ON event_dx_fac (encounter_id);
CREATE INDEX event_dx_fac_ibfk_5 ON event_dx_fac (user_id);
CREATE INDEX event_dx_fac_ibfk_6 ON event_dx_fac (work_item_id, encounter_id);
CREATE INDEX import_file_ibfk_2 ON import_file (work_item_id);
CREATE INDEX event_cdi_ibfk_2 ON event_cdi (user_id);
CREATE INDEX event_cdi_ibfk_3 ON event_cdi (qa_user_id);
CREATE INDEX event_cdi_ibfk_4 ON event_cdi (encounter_id);
CREATE INDEX event_cdi_ibfk_5 ON event_cdi (work_item_id);
CREATE INDEX assigned_provider_ibfk_1 ON assigned_provider (encounter_id);
CREATE INDEX charge_hold_cpt_ibfk_1 ON charge_hold_cpt (work_item_id, encounter_id);
CREATE INDEX fk_patient_status_id ON event_cpt_fac (patient_status_id);
CREATE INDEX idx_enc_id ON event_cpt_fac (encounter_id);
CREATE INDEX event_cpt_fac_ibfk_2 ON event_cpt_fac (procedure_provider);
CREATE INDEX event_cpt_fac_ibfk_6 ON event_cpt_fac (qa_user_id);
CREATE INDEX event_cpt_fac_ibfk_4 ON event_cpt_fac (qa_procedure_provider);
CREATE INDEX event_cpt_fac_ibfk_5 ON event_cpt_fac (user_id);
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
CREATE INDEX event_hcpcs_fac_ibfk_5 ON event_hcpcs_fac (qa_user_id);
CREATE INDEX event_hcpcs_fac_ibfk_7 ON event_hcpcs_fac (encounter_id);
CREATE INDEX event_hcpcs_fac_ibfk_9 ON event_hcpcs_fac (user_id);
CREATE INDEX work_item_id_2 ON event_hcpcs_fac (work_item_id, encounter_id);
CREATE INDEX fk_patient_status_id ON event_hcpcs_fac (patient_status_id);
CREATE INDEX event_hcpcs_fac_ibfk_2 ON event_hcpcs_fac (hcpcs_provider);
CREATE INDEX event_hcpcs_fac_ibfk_3 ON event_hcpcs_fac (qa_procedure_provider);
CREATE INDEX user_pubkey_ibfk_1 ON user_pubkey (user_id);
CREATE INDEX FK_client_dx_encounter_id ON client_dx (encounter_id);
CREATE INDEX FK_client_dx_transaction_id ON client_dx (transaction_id);
CREATE INDEX FK_client_dx_work_item_id ON client_dx (work_item_id);
CREATE INDEX event_icd10_dx_pro_ibfk_2 ON event_icd10_dx_pro (qa_user_id);
CREATE INDEX event_icd10_dx_pro_ibfk_3 ON event_icd10_dx_pro (work_item_id, encounter_id);
CREATE INDEX event_icd10_dx_pro_ibfk_4 ON event_icd10_dx_pro (dx_reason);
CREATE INDEX event_icd10_dx_pro_ibfk_5 ON event_icd10_dx_pro (code);
CREATE INDEX event_icd10_dx_pro_ibfk_1 ON event_icd10_dx_pro (user_id);
CREATE INDEX event_special_fac_ibfk_9 ON event_special_fac (qa_user_id);
CREATE INDEX event_special_fac_ibfk_11 ON event_special_fac (encounter_id);
CREATE INDEX event_special_fac_ibfk_4 ON event_special_fac (work_item_id, encounter_id);
CREATE INDEX event_special_fac_ibfk_5 ON event_special_fac (cosigning_physician);
CREATE INDEX event_special_fac_ibfk_6 ON event_special_fac (procedure_provider);
CREATE INDEX event_special_fac_ibfk_7 ON event_special_fac (qa_procedure_provider);
CREATE INDEX event_special_fac_ibfk_8 ON event_special_fac (user_id);
CREATE INDEX event_dx_pro_ibfk_3 ON event_dx_pro (qa_user_id);
CREATE INDEX event_dx_pro_ibfk_4 ON event_dx_pro (encounter_id);
CREATE INDEX event_dx_pro_ibfk_5 ON event_dx_pro (user_id);
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
CREATE INDEX fk_qa_user ON suspend_events (qa_user_id);
CREATE INDEX fk_user ON suspend_events (user_id);
CREATE INDEX idx_enc_id ON suspend_events (encounter_id);
CREATE INDEX suspend_events_ibfk_3 ON suspend_events (suspend_released_by);
CREATE INDEX work_item_em_level_ibfk_2 ON work_item_obs_codes (charge_type);
CREATE INDEX idx_client ON facilities (client_disposition_name);
`

func main() {
	db, err := postgres.Open("host=localhost user=postgres password=postgres dbname=clear_dev sslmode=disable", "index")
	if err != nil {
		log.Fatalln(err)
	}
	indexes := strings.Split(queries, ";")
	results := make(map[string][]string)
	f, err := os.OpenFile("indexes.sql", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	for _, index := range indexes {
		_, err := db.Exec(strings.TrimSpace(index))
		if err != nil {
			er := err.Error()
			results[er] = append(results[er], index)
		} else {
			results["success"] = append(results["success"], index)
		}
	}
	for err, indices := range results {
		if _, err := f.WriteString(fmt.Sprintf("*********** %s ************\n", err)); err != nil {
			panic(err)
		}
		for _, i := range indices {
			if _, err := f.WriteString(fmt.Sprintf("%s;\n", i)); err != nil {
				panic(err)
			}
		}
	}
}
