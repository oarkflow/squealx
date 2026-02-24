WITH procedures AS (
{{if alternate_client}}
SELECT
    proc,
    qty,
    mod1,
    mod2,
    dt,
    icd9dx as icd9_dx,
    icd10pcs as icd10_pcs,
    charge_code_select,
    pwi.alt_client_ref,
    p.first_name,
    p.last_name,
    p.middle_name,
    p.display_name,
    emFacId as em_fac_id,
    cptFacId as cpt_fac_id,
    emProId as em_prod_id,
    cptProId as cpt_prod_id,
    chargeType as charge_type,
    TO_CHAR(event_dos, 'YYYY-MM-DD') AS event_dos_short,
    patient_status_id,
    mod3,
    mod4
FROM (
         -- First UNION branch: event_em_pro
         SELECT
             em_level AS proc,
             1 AS qty,
             em_modifier1 AS mod1,
             em_modifier2 AS mod2,
             NULL AS dt,
             NULL AS icd9dx,
             NULL AS icd10pcs,
             charge_code_select,
             NULL::text AS emFacId,
             NULL::text AS cptFacId,
             event_em_pro_id AS emProId,
             NULL AS cptProId,
             billing_provider AS provider_lov,
             secondary_provider AS mid_lov,
             'ED_PROFEE' AS chargeType,
             event_dos,
             patient_status_id,
             procedure_modifier3 AS mod3,
             procedure_modifier4 AS mod4
         FROM event_em_pro
         WHERE em_level <> '0'
           AND work_item_id = @work_item_id
           AND encounter_id = @encounter_id

         UNION

         -- Second UNION branch: event_cpt_pro with GROUP BY
         SELECT
             procedure_num,
             procedure_qty,
             procedure_modifier1,
             procedure_modifier2,
             procedure_date,
             NULL AS icd9dx,
             NULL AS icd10pcs,
             charge_code_select,
             NULL::text AS emFacId,
             NULL::text AS cptFacId,
             event_cpt_pro_id,
             NULL AS cptProId,
             billing_provider,
             secondary_provider,
             'ED_PROFEE',
             CASE WHEN (wis.value::BOOLEAN = true) THEN event_dos ELSE procedure_date END AS event_dos,
             patient_status_id,
             procedure_modifier3 AS mod3,
             procedure_modifier4 AS mod4
         FROM event_cpt_pro cp
                  JOIN work_items twi ON cp.work_item_id = twi.work_item_id
                  LEFT JOIN work_item_settings wis ON wis.work_item_id = twi.work_item_id AND wis.key = 'multi_dos_encounters'
         WHERE cp.work_item_id = @work_item_id
           AND encounter_id = @encounter_id
         GROUP BY
             procedure_num,
             procedure_qty,
             procedure_modifier1,
             procedure_modifier2,
             procedure_date,
             billing_provider,
             secondary_provider,
             CASE
                 WHEN (wis.value::boolean = true)
                     THEN event_dos
                 ELSE procedure_date
             END,
             patient_status_id,
             procedure_modifier3,
             procedure_modifier4,
             charge_code_select,
             event_cpt_pro_id

         UNION

         -- Third UNION branch: event_special_pro
         SELECT
             procedure_num,
             procedure_qty,
             NULL AS mod1,
             NULL AS mod2,
             procedure_date,
             NULL AS icd9dx,
             NULL AS icd10pcs,
             NULL AS charge_code_select,
             NULL::text AS emFacId,
             NULL::text AS cptFacId,
             NULL AS emProId,
             NULL AS cptProId,
             procedure_provider,
             mid_provider,
             'ED_PROFEE',
             event_dos,
             NULL AS patient_status_id,
             NULL AS mod3,
             NULL AS mod4
         FROM event_special_pro
         WHERE work_item_id = @work_item_id
           AND encounter_id = @encounter_id

         UNION

         -- Fourth UNION branch: event_em_fac
         SELECT
             em_level,
             1,
             em_modifier1,
             em_modifier2,
             NULL,
             NULL,
             NULL,
             charge_code_select,
             CAST(event_em_fac_id AS text) AS emFacId,
             NULL::text AS cptFacId,
             NULL AS emProId,
             NULL AS cptProId,
             billing_provider,
             NULL,
             'ED_FACILITY',
             event_dos,
             patient_status_id,
             procedure_modifier3 AS mod3,
             procedure_modifier4 AS mod4
         FROM event_em_fac
         WHERE em_level <> '0'
           AND work_item_id = @work_item_id
           AND encounter_id = @encounter_id

         UNION

         -- Fifth UNION branch: event_cpt_fac with GROUP BY
         SELECT
             procedure_num,
             procedure_qty,
             procedure_modifier1,
             procedure_modifier2,
             procedure_date,
             procedure_dx,
             cp.icd10_pcs,
             charge_code_select,
             NULL::text AS emFacId,
             CAST(event_cpt_fac_id AS text) AS cptFacId,
             NULL AS emProId,
             NULL AS cptProId,
             billing_provider,
             NULL,
             'ED_FACILITY',
             CASE WHEN (wis.value::BOOLEAN = true) THEN event_dos ELSE procedure_date END AS event_dos,
             patient_status_id,
             procedure_modifier3 AS mod3,
             procedure_modifier4 AS mod4
         FROM event_cpt_fac cp
                  JOIN work_items twi ON cp.work_item_id = twi.work_item_id
                  LEFT JOIN work_item_settings wis ON wis.work_item_id = twi.work_item_id AND wis.key = 'multi_dos_encounters'
         WHERE cp.work_item_id = @work_item_id
           AND encounter_id = 22162472
         GROUP BY
             procedure_num,
             procedure_qty,
             procedure_modifier1,
             procedure_modifier2,
             procedure_date,
             billing_provider,
             secondary_provider,
             CASE
                 WHEN (wis.value::boolean = true)
                     THEN event_dos
                 ELSE procedure_date
             END,
             patient_status_id,
             procedure_modifier3,
             procedure_modifier4,
             procedure_dx,
             cp.icd10_pcs,
             charge_code_select,
             event_cpt_fac_id

         UNION

         -- Sixth UNION branch: event_special_fac
         SELECT
             procedure_num,
             procedure_qty,
             NULL AS mod1,
             NULL AS mod2,
             procedure_date,
             NULL AS icd9dx,
             NULL AS icd10pcs,
             NULL AS charge_code_select,
             NULL::text AS emFacId,
             NULL::text AS cptFacId,
             NULL AS emProId,
             NULL AS cptProId,
             procedure_provider,
             NULL,
             'ED_FACILITY',
             event_dos,
             NULL AS patient_status_id,
             NULL AS mod3,
             NULL AS mod4
         FROM event_special_fac
         WHERE work_item_id = @work_item_id
           AND encounter_id = @encounter_id
     ) AS Event
         JOIN work_items
              ON work_items.work_item_id = @work_item_id
         LEFT JOIN work_item_settings wis
                   ON wis.work_item_id = work_items.work_item_id AND wis.key = 'billing_mlp'
         LEFT JOIN providers p
                   ON p.provider_lov = CASE
                                           WHEN ((wis.value::boolean = true) AND mid_lov <> '')
                                               THEN mid_lov
                                           ELSE Event.provider_lov
                                       END
         LEFT JOIN vw_provider_wi pwi
                   ON pwi.work_item_id = @work_item_id
                      AND pwi.provider_id = p.provider_id;


{{else}}

SELECT
    proc,
    qty,
    mod1,
    mod2,
    dt,
    icd9dx as icd9_dx,
    icd10pcs as icd10_pcs,
    charge_code_select,
    pwi.client_ref,
    p.first_name,
    p.last_name,
    p.middle_name,
    p.display_name,
    emFacId as em_fac_id,
    cptFacId as cpt_fac_id,
    emProId as em_prod_id,
    cptProId as cpt_prod_id,
    chargeType as charge_type,
    TO_CHAR(event_dos, 'YYYY-MM-DD') AS event_dos_short,
    patient_status_id,
    mod3,
    mod4
FROM (
         -- First UNION branch: event_em_pro
         SELECT
             em_level AS proc,
             1 AS qty,
             em_modifier1 AS mod1,
             em_modifier2 AS mod2,
             NULL::date AS dt,
             NULL::text AS icd9dx,
             NULL::text AS icd10pcs,
             charge_code_select,
             NULL::text AS emFacId,
             NULL::text AS cptFacId,
             event_em_pro_id AS emProId,
             NULL::bigint AS cptProId,
             billing_provider AS provider_lov,
             secondary_provider AS mid_lov,
             'ED_PROFEE' AS chargeType,
             event_dos,
             patient_status_id,
             procedure_modifier3 AS mod3,
             procedure_modifier4 AS mod4
         FROM event_em_pro
         WHERE em_level <> '0'
           AND work_item_id = @work_item_id
           AND encounter_id = @encounter_id

         UNION

         -- Second UNION branch: event_cpt_pro with GROUP BY
         SELECT
             procedure_num,
             procedure_qty,
             procedure_modifier1,
             procedure_modifier2,
             procedure_date,
             NULL AS icd9dx,
             NULL AS icd10pcs,
             charge_code_select,
             NULL::text AS emFacId,
             NULL::text AS cptFacId,
             event_cpt_pro_id,
             NULL::bigint AS cptProId,
             billing_provider,
             secondary_provider,
             'ED_PROFEE',
             CASE WHEN (wis.value::BOOLEAN = true) THEN event_dos ELSE procedure_date END AS event_dos,
             patient_status_id,
             procedure_modifier3 AS mod3,
             procedure_modifier4 AS mod4
         FROM event_cpt_pro cp
         JOIN work_items twi ON cp.work_item_id = twi.work_item_id
         LEFT JOIN work_item_settings wis
           ON wis.work_item_id = twi.work_item_id
          AND wis.key = 'multi_dos_encounters'
         WHERE cp.work_item_id = @work_item_id
           AND encounter_id = @encounter_id
         GROUP BY
             procedure_num,
             procedure_qty,
             procedure_modifier1,
             procedure_modifier2,
             procedure_date,
             billing_provider,
             secondary_provider,
             CASE
                 WHEN (wis.value::boolean = true)
                     THEN event_dos
                 ELSE procedure_date
             END,
             patient_status_id,
             procedure_modifier3,
             procedure_modifier4,
             charge_code_select,
             event_cpt_pro_id

         UNION

         -- Third UNION branch: event_special_pro
         SELECT
             procedure_num,
             procedure_qty,
             NULL::text AS mod1,
             NULL::text AS mod2,
             procedure_date,
             NULL::text AS icd9dx,
             NULL::text AS icd10pcs,
             NULL::text AS charge_code_select,
             NULL::text AS emFacId,
             NULL::text AS cptFacId,
             NULL::bigint AS emProId,
             NULL::bigint AS cptProId,
             procedure_provider,
             mid_provider,
             'ED_PROFEE',
             event_dos,
             NULL::bigint AS patient_status_id,
             NULL::text AS mod3,
             NULL::text AS mod4
         FROM event_special_pro
         WHERE work_item_id = @work_item_id
           AND encounter_id = @encounter_id

         UNION

         -- Fourth UNION branch: event_em_fac
         SELECT
             em_level,
             1,
             em_modifier1,
             em_modifier2,
             NULL::date,
             NULL::text,
             NULL::text,
             charge_code_select,
             event_em_fac_id::text AS emFacId,
             NULL::text AS cptFacId,
             NULL::bigint AS emProId,
             NULL::bigint AS cptProId,
             billing_provider,
             NULL::text,
             'ED_FACILITY',
             event_dos,
             patient_status_id,
             procedure_modifier3 AS mod3,
             procedure_modifier4 AS mod4
         FROM event_em_fac
         WHERE em_level <> '0'
           AND work_item_id = @work_item_id
           AND encounter_id = @encounter_id

         UNION

         -- Fifth UNION branch: event_cpt_fac with GROUP BY
         SELECT
             procedure_num,
             procedure_qty,
             procedure_modifier1,
             procedure_modifier2,
             procedure_date,
             procedure_dx,
             cp.icd10_pcs,
             charge_code_select,
             NULL::text AS emFacId,
             CAST(event_cpt_fac_id AS text) AS cptFacId,
             NULL::bigint AS emProId,
             NULL::bigint AS cptProId,
             billing_provider,
             NULL,
             'ED_FACILITY',
             CASE WHEN (wis.value::BOOLEAN = true) THEN event_dos ELSE procedure_date END AS event_dos,
             patient_status_id,
             procedure_modifier3 AS mod3,
             procedure_modifier4 AS mod4
         FROM event_cpt_fac cp
         JOIN work_items twi ON cp.work_item_id = twi.work_item_id
         LEFT JOIN work_item_settings wis
           ON wis.work_item_id = twi.work_item_id
          AND wis.key = 'multi_dos_encounters'
         WHERE cp.work_item_id = @work_item_id
           AND encounter_id = 22162472
         GROUP BY
             procedure_num,
             procedure_qty,
             procedure_modifier1,
             procedure_modifier2,
             procedure_date,
             billing_provider,
             secondary_provider,
             CASE
                 WHEN (wis.value::boolean = true)
                     THEN event_dos
                 ELSE procedure_date
             END,
             patient_status_id,
             procedure_modifier3,
             procedure_modifier4,
             procedure_dx,
             cp.icd10_pcs,
             charge_code_select,
             event_cpt_fac_id

         UNION

         -- Sixth UNION branch: event_special_fac
         SELECT
             procedure_num,
             procedure_qty,
             NULL::text AS mod1,
             NULL::text AS mod2,
             procedure_date,
             NULL::text,
             NULL::text,
             NULL::text,
             NULL::text,
             NULL::text,
             NULL::bigint AS emProId,
             NULL::bigint AS cptProId,
             procedure_provider,
             NULL::text,
             'ED_FACILITY',
             event_dos,
             NULL::bigint AS patient_status_id,
             NULL::text AS mod3,
             NULL::text AS mod4
         FROM event_special_fac
         WHERE work_item_id = @work_item_id
           AND encounter_id = @encounter_id
           {{if mips_as_changes}}

        UNION
           SELECT
            pp.pqrs_num as proc,
            1 as qty,
            pp.modifier as mod1,
            '' as mod2,
            NULL AS dt,
            NULL AS icd9dx,
            NULL AS icd10pcs,
            ep.charge_code_select,
            NULL AS emFacId,
            NULL AS cptFacId,
            pp.event_pqrs_pro_id AS emProId,
            NULL AS cptProId,
            ep.billing_provider AS provider_lov,
            ep.secondary_provider AS mid_lov,
            'ED_PROFEE' AS chargeType,
            ep.event_dos,
            NULL as patient_status_id,
            NULL as mod3,
            NULL as mod4
        FROM
            event_pqrs_pro pp
            LEFT JOIN event_em_pro ep ON pp.work_item_id = ep.work_item_id
            AND pp.encounter_id = ep.encounter_id
        WHERE
            pp.work_item_id = @work_item_id
            AND pp.encounter_id = @encounter_id
            AND pqrs_num NOT LIKE '%NOTMET%'
            {{end}}
     ) AS Event
JOIN work_items ON work_items.work_item_id = @work_item_id
LEFT JOIN work_item_settings wis
  ON wis.work_item_id = work_items.work_item_id
 AND wis.key = 'billing_mlp'
LEFT JOIN providers p
    ON p.provider_lov = CASE
                           WHEN ((wis.value::boolean = true) AND mid_lov <> '')
                             THEN mid_lov
                           ELSE Event.provider_lov
                        END
LEFT JOIN vw_provider_wi pwi
    ON pwi.work_item_id = @work_item_id
    AND pwi.provider_id = p.provider_id

{{end}}

),
mapped_procs AS (
  -- For each procedure, attempt to find a matching charge mapping
  SELECT
    p.*,
    cm.charge_master_id,
    (cm.charge_master_id IS NOT NULL) AS charge_code_mapped
  FROM procedures p
  LEFT JOIN LATERAL (
      SELECT cm.charge_master_id
      FROM (
          SELECT work_item_id, charge_type, MAX(effective_date) AS effective_date, patient_status_id
          FROM charge_master
          WHERE work_item_id = @work_item_id
            AND charge_type = p.charge_type
            AND effective_date <= p.event_dos_short::DATE
          GROUP BY work_item_id, charge_type, patient_status_id
      ) AS EffectiveDate
      JOIN charge_master cm USING (work_item_id, charge_type, effective_date)
      WHERE cm.cpt_hcpcs_code IN (
          CONCAT(p.proc, p.mod1, p.mod2),
          CONCAT(p.proc, p.mod2, p.mod1),
          CONCAT(p.proc, p.mod1),
          CONCAT(p.proc, p.mod2),
          p.proc
      )
        AND cm.client_internal_code <> ''
      ORDER BY LENGTH(cm.cpt_hcpcs_code) DESC
      LIMIT 1
  ) cm ON TRUE
),
unmapped_procs AS (
  -- Select only procedures that did not get a mapping
  SELECT * FROM mapped_procs WHERE NOT charge_code_mapped
),
proc_counts AS (
  SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE NOT charge_code_mapped) AS unmapped
  FROM mapped_procs
),
-- If there are unmapped procedures, insert them into an unmapped procedure log table
ins_unmapped AS (
  INSERT INTO charge_hold (work_item_id, encounter_id, cpt, charge_type)
  SELECT @work_item_id, @encounter_id, proc, charge_type
  FROM unmapped_procs
  WHERE EXISTS (SELECT 1 FROM unmapped_procs)
  RETURNING 1
),
-- Similarly, if there are unmapped procedures, insert a suspend record
ins_suspend AS (
  INSERT INTO suspend_events (work_item_id, encounter_id, suspend_reason, suspend_note, visible, suspend_provider, user_id, user_time_entered)
  SELECT @work_item_id, @encounter_id,
         CASE
           WHEN charge_type = 'PhysicianBilling' THEN 'Pro CPT Missing Charge Code And/Or Amount'
           ELSE 'Tech CPT Missing CDM Number and Charge Amount'
         END,
         'Unmapped procedure: ' || proc,
         true, '', @user_id, now()
  FROM unmapped_procs
  WHERE EXISTS (SELECT 1 FROM unmapped_procs)
  RETURNING 1
),
-- Update encounter details only if there are unmapped procedures
upd_enc AS (
  UPDATE encounter_details
  SET encounter_status = 'SUSPEND'
  WHERE work_item_id = @work_item_id AND encounter_id = @encounter_id
    AND EXISTS (SELECT 1 FROM unmapped_procs)
  RETURNING 1
),
-- Optionally, log an activity entry (can be done unconditionally)
ins_activity AS (
  INSERT INTO encounter_activities (encounter_id, user_id, new_status, comment, work_item_id)
  VALUES (@encounter_id, @user_id, 'SUSPEND', 'Charge Entry Hold', @work_item_id)
  RETURNING 1
)
SELECT
  pc.total AS total_procedure_count,
  pc.unmapped AS unmapped_procedure_count,
  (pc.unmapped = 0) AS valid
FROM proc_counts pc;
