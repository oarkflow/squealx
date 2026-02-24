WITH disagree AS (
  -- Sum qa_disagree values from multiple tables
  SELECT COALESCE(SUM(qa_disagree_int), 0) AS disagreeCount
  FROM (
    SELECT CASE WHEN qa_disagree THEN 1 ELSE 0 END AS qa_disagree_int FROM event_em_pro            WHERE work_item_id = @work_item_id AND encounter_id = @encounter_id
    UNION ALL
    SELECT CASE WHEN qa_disagree THEN 1 ELSE 0 END AS qa_disagree_int FROM event_cpt_pro           WHERE work_item_id = @work_item_id AND encounter_id = @encounter_id
    UNION ALL
    SELECT CASE WHEN qa_disagree THEN 1 ELSE 0 END AS qa_disagree_int FROM event_special_pro       WHERE work_item_id = @work_item_id AND encounter_id = @encounter_id
    UNION ALL
    SELECT CASE WHEN qa_disagree THEN 1 ELSE 0 END AS qa_disagree_int FROM event_dx_pro            WHERE work_item_id = @work_item_id AND encounter_id = @encounter_id
    UNION ALL
    SELECT CASE WHEN qa_disagree THEN 1 ELSE 0 END AS qa_disagree_int FROM event_icd10_dx_pro       WHERE work_item_id = @work_item_id AND encounter_id = @encounter_id
    UNION ALL
    SELECT CASE WHEN qa_disagree THEN 1 ELSE 0 END AS qa_disagree_int FROM event_hcpcs_pro          WHERE work_item_id = @work_item_id AND encounter_id = @encounter_id
    UNION ALL
    SELECT CASE WHEN qa_disagree THEN 1 ELSE 0 END AS qa_disagree_int FROM event_pqrs_pro           WHERE encounter_id = @encounter_id
    UNION ALL
    SELECT CASE WHEN qa_disagree THEN 1 ELSE 0 END AS qa_disagree_int FROM event_em_fac             WHERE work_item_id = @work_item_id AND encounter_id = @encounter_id
    UNION ALL
    SELECT CASE WHEN qa_disagree THEN 1 ELSE 0 END AS qa_disagree_int FROM event_cpt_fac            WHERE work_item_id = @work_item_id AND encounter_id = @encounter_id
    UNION ALL
    SELECT CASE WHEN qa_disagree THEN 1 ELSE 0 END AS qa_disagree_int FROM event_special_fac        WHERE work_item_id = @work_item_id AND encounter_id = @encounter_id
    UNION ALL
    SELECT CASE WHEN qa_disagree THEN 1 ELSE 0 END AS qa_disagree_int FROM event_dx_fac             WHERE work_item_id = @work_item_id AND encounter_id = @encounter_id
    UNION ALL
    SELECT CASE WHEN qa_disagree THEN 1 ELSE 0 END AS qa_disagree_int FROM event_icd10_dx_fac        WHERE work_item_id = @work_item_id AND encounter_id = @encounter_id
    UNION ALL
    SELECT CASE WHEN qa_disagree THEN 1 ELSE 0 END AS qa_disagree_int FROM event_hcpcs_fac           WHERE work_item_id = @work_item_id AND encounter_id = @encounter_id
    UNION ALL
    SELECT CASE WHEN qa_disagree THEN 1 ELSE 0 END AS qa_disagree_int FROM event_cdi_fac             WHERE encounter_id = @encounter_id
  ) AS t
),
codes_changed AS (
  -- Check if any history record indicates a QA change
  SELECT EXISTS(
    SELECT 1
    FROM encounter_history
    WHERE work_item_id = @work_item_id
      AND encounter_id = @encounter_id
      AND detail LIKE 'QA%'
  ) AS codes_changed
),
notes_added AS (
  -- Check if any coder notes exist from the QA user (assumed to be @user_id)
  SELECT EXISTS(
    SELECT 1
    FROM coder_notes
    WHERE work_item_id = @work_item_id
      AND encounter_id = @encounter_id
      AND user_id = @user_id
  ) AS notes_added
),
qa_feedback_flag AS (
  -- qa_feedback is TRUE if any disagree exists or codes changed or notes added.
  SELECT CASE
           WHEN ((SELECT disagreeCount FROM disagree) > 0)
                OR (SELECT codes_changed FROM codes_changed)
                OR (SELECT notes_added FROM notes_added)
           THEN TRUE
           ELSE FALSE
         END AS qa_feedback
),
detail_cte AS (
  -- Get the detail record (assumes a single record per encounter)
  SELECT qa_dataentry, dataentry_complete_tec, dataentry_complete_pf, dataentry_assigned, encounter_status
  FROM encounter_details
  WHERE work_item_id = @work_item_id AND encounter_id = @encounter_id
  LIMIT 1
),
new_status_calc AS (
  -- Calculate the new status based on the QA dataentry flag and completion flags.
  -- Note: @split_data_entry and @live_code_split are parameters (booleans) you would bind.
  SELECT
    d.qa_dataentry,
    d.dataentry_complete_tec,
    d.dataentry_complete_pf,
    CASE
      WHEN d.qa_dataentry != false AND @split_data_entry = TRUE THEN
         CASE
           WHEN @live_code_split = TRUE THEN
             CASE WHEN d.dataentry_complete_pf = FALSE THEN 'DE_PRO_READY'
                  WHEN d.dataentry_complete_tec = FALSE THEN 'DE_TEC_READY'
                  ELSE 'DE_COMPLETE'
             END
           ELSE
             CASE WHEN d.dataentry_complete_tec = FALSE THEN 'DE_TEC_READY'
                  WHEN d.dataentry_complete_pf = FALSE THEN 'DE_PRO_READY'
                  ELSE 'DE_COMPLETE'
             END
         END
      ELSE 'COMPLETE'
    END AS new_status,
    d.dataentry_assigned
  FROM detail_cte d
),
upd AS (
  -- Update the detail_record with new status and QA fields.
  UPDATE encounter_details
  SET encounter_status   = ns.new_status,
      qa_complete        = true,
      qa_feedback        = (SELECT qa_feedback FROM qa_feedback_flag),
      qa_complete_ts     = now(),
      dataentry_assigned = NULL  -- or your designated “not assigned” value
  FROM new_status_calc ns
  WHERE work_item_id = @work_item_id AND encounter_id = @encounter_id
  RETURNING *
),
act AS (
  -- Insert an encounter_activities log entry.
  INSERT INTO encounter_activities (encounter_id, user_id, new_status, comment, work_item_id)
  VALUES (@encounter_id, @user_id, (SELECT new_status FROM new_status_calc), 'QA Complete', @work_item_id)
  RETURNING *
)
SELECT * FROM upd;
