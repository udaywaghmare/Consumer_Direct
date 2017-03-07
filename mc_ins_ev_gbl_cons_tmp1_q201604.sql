SELECT COUNT(1) FROM sas_pulse_stg.ev_gbl_cons_nps_tmp1_prev_q; --7193250
SELECT COUNT(1) FROM sas_pulse_stg.ev_gbl_cons_nps_tmp1; ---6921675

DELETE sas_pulse_stg.ev_gbl_cons_nps_tmp1_prev_q ALL;
INSERT INTO sas_pulse_stg.ev_gbl_cons_nps_tmp1_prev_q
SELECT * FROM sas_pulse_stg.ev_gbl_cons_nps_tmp1;

DELETE FROM sas_pulse_stg.ev_gbl_cons_nps_tmp1;

INSERT INTO sas_pulse_stg.ev_gbl_cons_nps_tmp1 
(
	evnt_ldr_id,
	evnt_type_cd,   
	evnt_subtype_cd,
	evnt_dt,
	evnt_dts,
	cnsld_src_txn_id,
	cnsld_src_txn_bu_id,
	so_nbr,
	so_bu_id,
	so_lcl_chnl_cd,
	so_dt,
	so_inv_dt,
	qte_nbr,
	qte_bu_id,
	qte_lcl_chnl_cd,
	qte_dt,
	prim_sls_assoc_nbr,
	secnd_sls_assoc_nbr,
	sys_itm_cls_nm,
	sys_flg,
	snp_flg,
	svcs_flg,
	sls_chnl_cd,
	tot_revn_disc_amt,
	ord_dispos_stat_cd,
	sls_type_cd,
	cust_bu_id,
	cust_lcl_chnl_cd,
	bilt_cust_nbr,
	bilt_src_cust_id,
	bilt_addr_seq_id,
	bilt_cntct_id,
	shpto_cust_nbr,
	shpto_src_cust_id,
	shpto_addr_seq_id,
	shpto_cntct_id)
SELECT
	320 AS evnt_ldr_id,  --:evnt_ldr_id,
	'CONSNPS' AS evnt_type_cd,
	'FY'||(SELECT  TRIM(y.fiscal_year) FROM   CORP.FISCAL_YEAR_INFO y WHERE CURRENT_DATE BETWEEN y.BEGIN_DATE AND y.END_DATE GROUP BY 1) ||
     'Q'||(SELECT  TRIM(q.FISCAL_QUARTER) FROM   CORP.FISCAL_QUARTER_INFO q WHERE CURRENT_DATE BETWEEN q.BEGIN_DATE AND q.END_DATE GROUP BY 1) || 	
	 'M'||(SELECT  TRIM(m.FISCAL_MONTH) FROM   CORP.FISCAL_MONTH_INFO m WHERE CURRENT_DATE BETWEEN m.BEGIN_DATE AND m.END_DATE GROUP BY 1) ||
	 'W'||(SELECT SUBSTR(TRIM(FISCAL_WEEK), 5,6) FROM corp.FISCAL_DAY d WHERE d.ACTUAL_DATE = CURRENT_DATE) ||
	 'D' ||(SELECT TRIM(FISCAL_DAY_OF_WEEK_CODE) FROM corp.FISCAL_DAY d2 WHERE d2.ACTUAL_DATE = CURRENT_DATE)
	AS evnt_subtype_cd,
	shf.ord_dt AS evnt_dt,    
	shf.ord_dts AS evnt_dts,
	shf.ord_nbr,
	shf.src_bu_id,
	shf.ord_nbr,
	shf.src_bu_id,
	shf.src_lcl_chnl_cd,
	shf.ord_dt,
	shf.inv_dt,
	shf.qte_nbr,
	CASE WHEN shf.qte_nbr IS NOT NULL THEN shf.src_bu_id ELSE NULL END AS qte_bu_id,
	CASE WHEN shf.qte_nbr IS NOT NULL THEN shf.src_lcl_chnl_cd ELSE NULL END AS qte_lcl_chnl_cd,
	CASE WHEN shf.qte_nbr IS NOT NULL THEN shf.ord_dt ELSE NULL END AS qte_dt,
	shf.prim_sls_assoc_nbr,
	shf.secnd_sls_assoc_nbr,
	NULL AS sys_itm_cls_nm,
	NULL AS sys_flg,
	NULL AS snp_flg, 
	NULL AS svcs_flg, -- may need to be changed
	--CASE WHEN COALESCE(shf.onln_lvl_cd, shf.intrnt_revn_cd,'N') IN ('2','N') THEN 'OFFLINE' ELSE 'ONLINE' END AS sls_chnl_cd,
	CASE WHEN onln_flg.onln_desc IS NOT NULL THEN 'ONLINE' ELSE 'OFFLINE' end AS sls_chnl_cd,
	SUM(odf.revn_disc_txn_amt *(CASE WHEN odf.revn_usd_rt IS NULL OR odf.revn_usd_rt = 0 THEN 1 ELSE odf.revn_usd_rt END)) AS tot_revn_disc_amt, 
	shf.ord_dspsn_stat_cd AS ord_dispos_stat_cd,
	NULL AS sls_type_cd,
	shf.ref_bu_id,
	shf.ref_lcl_chnl_cd,
	shf.sldt_cust_nbr,
	shf.sldt_src_cust_id,
	CASE WHEN shf.sldt_cust_nbr = shf.bilt_cust_nbr THEN shf.bilt_addr_seq_id
		WHEN shf.sldt_cust_nbr = shf.shpto_cust_nbr THEN shf.shpto_addr_seq_id
		ELSE NULL end AS sldto_addr_seq_id,
	shf.sldt_cntct_id,
	shf.shpto_cust_nbr,
	shf.shpto_src_cust_id,
	shf.shpto_addr_seq_id,
	shf.shpto_cntct_id
FROM sls_pkg.so_hdr_fact shf	
INNER JOIN sls_pkg.so_dtl_fact odf ON shf.ORD_NBR = odf.ORD_NBR AND shf.SRC_BU_ID = odf.SRC_BU_ID
INNER JOIN itm_pkg.comb_prod_hier cph ON odf.extrnl_comb_hier_cd = cph.comb_hier_cd
INNER JOIN corp_drm_pkg.phys_geo_hier pgh ON shf.ref_bu_id = pgh.bu_id
INNER JOIN corp_drm_pkg.chnl_hier ch ON shf.ref_bu_id = ch.bu_id AND shf.ref_lcl_chnl_cd = ch.lcl_chnl_code	
LEFT OUTER JOIN SLS_BASE.ORD_ONLN_OFFLINE_CATG onln_flg ON odf.ORD_NBR = onln_flg.ord_nbr AND  odf.SRC_BU_ID = onln_flg.src_bu_id
WHERE 
	shf.ord_type_cd = 'I'
	AND shf.ord_inv_ind = 'A'
	AND shf.ord_dspsn_stat_cd = 'CLOSED'
	AND (shf.ord_dt BETWEEN '2016-02-07' AND '2017-02-05') 
/*	AND (shf.ord_dt BETWEEN '2016-01-02' AND '2017-01-07') */
/*	AND (shf.ord_dt BETWEEN '2015-12-05' AND '2016-12-04') */
	AND pgh.ISO_CTRY_CODE_2 IN ('US','CA','BR','MX','GB','FR','DE','JP','CN','IN','AU','NZ','SE') 
	AND ((ch.CUST_TYPE_CODE = 'CBOCH' AND ch.SEG_CODE <> 'COSLR') OR (ch.BU_ID = 3232 AND ch.LCL_CHNL_CODE = 'OH')
			OR ( ch.lcl_chnl_code in ('SEOFF','1801','EPP','48274') and ch. bu_id in (1212, 4065)))
	AND cph.BRAND_CATG_DESC NOT IN ('UNKNOWN', 'SPARES')	
	AND odf.tmzn_loc_id IN (1,2,3,4,5) 
	AND shf.tmzn_loc_id IN (1,2,3,4,5)
GROUP BY 
	shf.ord_dt,    
	shf.ord_dts,
	shf.ord_nbr,
	shf.src_bu_id,
	shf.src_lcl_chnl_cd,
	shf.inv_dt,
	shf.qte_nbr,
	CASE WHEN shf.qte_nbr IS NOT NULL THEN shf.src_bu_id ELSE NULL END,
	CASE WHEN shf.qte_nbr IS NOT NULL THEN shf.src_lcl_chnl_cd ELSE NULL END,
	CASE WHEN shf.qte_nbr IS NOT NULL THEN shf.ord_dt ELSE NULL END,
	shf.prim_sls_assoc_nbr,
	shf.secnd_sls_assoc_nbr,
	--CASE WHEN COALESCE(shf.onln_lvl_cd, shf.intrnt_revn_cd,'N') IN ('2','N') THEN 'OFFLINE' ELSE 'ONLINE' END,
	CASE WHEN onln_flg.onln_desc IS NOT NULL THEN 'ONLINE' ELSE 'OFFLINE' end,
	shf.ord_dspsn_stat_cd,
	shf.ref_bu_id,
	shf.ref_lcl_chnl_cd,
	shf.sldt_cust_nbr,
	shf.sldt_src_cust_id,
	CASE WHEN shf.sldt_cust_nbr = shf.bilt_cust_nbr THEN shf.bilt_addr_seq_id
		WHEN shf.sldt_cust_nbr = shf.shpto_cust_nbr THEN shf.shpto_addr_seq_id
		ELSE NULL end,
	shf.sldt_cntct_id,
	shf.shpto_cust_nbr,
	shf.shpto_src_cust_id,
	shf.shpto_addr_seq_id,
	shf.shpto_cntct_id
HAVING  tot_revn_disc_amt > 0;

--6161313 00:01:14	

