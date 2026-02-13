
SELECT
  rep.cntr_code AS `country`,
  (col_placeholder),
  rep.dmat_div_code AS `division`,
  rep.dmat_div_des AS `division_name`,
  rep.dmat_dep_code AS `department`,
  rep.dmat_dep_des AS `department_name`,
  rep.dmat_sec_code AS `section`,
  rep.dmat_sec_des AS `section_name`,
  rep.dmat_grp_code AS `group`,
  rep.dmat_grp_des AS `group_name`,

  rc.rc_cat_name AS rpc_category,

  /* --- Alap metrikák (TY/LY) --- */
  COALESCE(SUM(CASE WHEN t.ev = 'ty' THEN sales.slsms_salex_cs / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `sales_excl_vat_gbp_ty`,
  COALESCE(SUM(CASE WHEN t.ev = 'ty' THEN sales.slsms_margin   / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `scan_margin_gbp_ty`,
  COALESCE(SUM(CASE WHEN t.ev = 'ty' THEN sales.slsms_unit_cs                              ELSE 0 END), 0) AS `sold_unit_ty`,

  COALESCE(SUM(CASE WHEN t.ev = 'ly' THEN sales.slsms_salex_cs / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `sales_excl_vat_gbp_ly`,
  COALESCE(SUM(CASE WHEN t.ev = 'ly' THEN sales.slsms_margin   / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `scan_margin_gbp_ly`,
  COALESCE(SUM(CASE WHEN t.ev = 'ly' THEN sales.slsms_unit_cs                              ELSE 0 END), 0) AS `sold_unit_ly`,

  /* --- LFL metrikák (TY/LY) --- */
  COALESCE(SUM(CASE WHEN t.ev = 'ty' AND sales.slsms_lfl_flag = 1
                    THEN sales.slsms_salex_cs / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `sales_excl_vat_gbp_lfl`,
  COALESCE(SUM(CASE WHEN t.ev = 'ty' AND sales.slsms_lfl_flag = 1
                    THEN sales.slsms_margin   / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `scan_margin_gbp_lfl`,
  COALESCE(SUM(CASE WHEN t.ev = 'ty' AND sales.slsms_lfl_flag = 1
                    THEN sales.slsms_unit_cs                              ELSE 0 END), 0) AS `sold_unit_lfl`,

  COALESCE(SUM(CASE WHEN t.ev = 'ly' AND sales.slsms_lfl_flag = 1
                    THEN sales.slsms_salex_cs / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `sales_excl_vat_gbp_lflb`,
  COALESCE(SUM(CASE WHEN t.ev = 'ly' AND sales.slsms_lfl_flag = 1
                    THEN sales.slsms_margin   / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `scan_margin_gbp_lflb`,
  COALESCE(SUM(CASE WHEN t.ev = 'ly' AND sales.slsms_lfl_flag = 1
                    THEN sales.slsms_unit_cs                              ELSE 0 END), 0) AS `sold_unit_lflb`,

  /* --- 2Y LFL metrikák (opcionális, ha szükséges) --- */
  COALESCE(SUM(CASE WHEN t.ev = 'ty' AND sales.slsms_2ylfl_flag = 1
                    THEN sales.slsms_salex_cs / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `sales_excl_vat_gbp_ty_2ylfl`,
  COALESCE(SUM(CASE WHEN t.ev = 'ty' AND sales.slsms_2ylfl_flag = 1
                    THEN sales.slsms_margin   / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `scan_margin_gbp_ty_2ylfl`,
  COALESCE(SUM(CASE WHEN t.ev = 'ty' AND sales.slsms_2ylfl_flag = 1
                    THEN sales.slsms_unit_cs                              ELSE 0 END), 0) AS `sold_unit_ty_2ylfl`,

  COALESCE(SUM(CASE WHEN t.ev = 'ly' AND sales.slsms_2ylfl_flag = 1
                    THEN sales.slsms_salex_cs / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `sales_excl_vat_gbp_2ylflb`,
  COALESCE(SUM(CASE WHEN t.ev = 'ly' AND sales.slsms_2ylfl_flag = 1
                    THEN sales.slsms_margin   / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `scan_margin_gbp_2ylflb`,
  COALESCE(SUM(CASE WHEN t.ev = 'ly' AND sales.slsms_2ylfl_flag = 1
                    THEN sales.slsms_unit_cs                              ELSE 0 END), 0) AS `sold_unit_2ylflb`

FROM dm.dim_artrep_details rep

JOIN dw.sl_sms sales
  ON sales.slsms_dmat_id = rep.slad_dmat_id
 AND sales.slsms_cntr_id IN (1, 2, 4)
 AND sales.slsms_cntr_id = rep.cntr_id

/* Ha szükséges, nyisd vissza a store join-t:
JOIN dm.dim_stores store
  ON sales.slsms_dmst_id = store.dmst_store_id
 AND sales.slsms_cntr_id = store.cntr_id
*/

JOIN (
  SELECT
    'ty' AS ev,
    dmtm_fw_weeknum AS het,
    dmtm_d_code     AS nap,
    dmtm_fw_code
  FROM dm.dim_time_d
  WHERE dmtm_fw_code BETWEEN 'start_week' AND 'end_week'

  UNION
  SELECT
    'ly' AS ev,
    dmtm_fw_weeknum      AS het,
    dmtm_d_code_ly_offset AS nap,
    dmtm_fw_code
  FROM dm.dim_time_d
  WHERE dmtm_fw_code BETWEEN 'start_week' AND 'end_week'
) t
  ON t.nap = sales.part_col

JOIN (
  SELECT
    dmexr_cntr_id,
    dmexr_rate
  FROM dw.dim_exchange_rates
  WHERE dmexr_dmtm_fy_code = (SELECT fiscal_year FROM tesco_analysts.pmajor1_fiscal_year_for_filter)
    AND dmexr_crncy_to = 'GBP'
    AND dmexr_cntr_id IN (1, 2, 4)
) rate
  ON sales.slsms_cntr_id = rate.dmexr_cntr_id

JOIN (
  SELECT
    dmrrc_cntr_id,
    dmrrc_code_id,
    rc_cat_name
  FROM dw.dim_retail_rc
  JOIN dm.dim_rc_ret_category ON rc_cat_id = dmrrc_rc_cat_id
  GROUP BY
    dmrrc_cntr_id,
    dmrrc_code_id,
    rc_cat_name
) rc
  ON sales.slsms_cntr_id = rc.dmrrc_cntr_id
 AND sales.slsms_nrc     = rc.dmrrc_code_id

WHERE  rep.cntr_code IN ('HU','CZ','SK')

GROUP BY
  (group_placeholder),
  rep.cntr_code,
  rep.dmat_div_code,
  rep.dmat_div_des,
  rep.dmat_dep_code,
  rep.dmat_dep_des,
  rep.dmat_sec_code,
  rep.dmat_sec_des,
  rep.dmat_grp_code,
  rep.dmat_grp_des,
  rc.rc_cat_name;
