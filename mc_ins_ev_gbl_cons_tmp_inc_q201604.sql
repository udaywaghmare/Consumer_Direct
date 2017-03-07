Select Count(1) From sas_pulse_stg.ev_gbl_cons_nps_inc_prev_q; ---3713924
Select Count(1) From sas_pulse_stg.ev_gbl_cons_nps_inc; ---3399685

Delete sas_pulse_stg.ev_gbl_cons_nps_inc_prev_q All;
Insert into sas_pulse_stg.ev_gbl_cons_nps_inc_prev_q
Select * From sas_pulse_stg.ev_gbl_cons_nps_inc;

CREATE MULTISET TABLE tmp_work_db.ph_lkup ,FALLBACK ,NO BEFORE JOURNAL,NO AFTER JOURNAL,
CHECKSUM = DEFAULT,DEFAULT MERGEBLOCKRATIO
( 
      CNSLD_SRC_TXN_ID VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC,
      CNSLD_SRC_TXN_BU_ID INTEGER,
      ph_nbr VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC)
UNIQUE PRIMARY INDEX UPI_ph_lkup(CNSLD_SRC_TXN_ID,CNSLD_SRC_TXN_BU_ID
);

DELETE FROM tmp_work_db.ph_lkup ALL;
INSERT INTO tmp_work_db.ph_lkup 
SELECT  tmp3.cnsld_src_txn_id,
        tmp3.cnsld_src_txn_bu_id,
        MAX(CAST(TRIM(COALESCE(bc.ph_nbr, sc.ph_nbr)) AS VARCHAR(254))) AS ph_nbr
    FROM sas_pulse_stg.ev_gbl_cons_nps_tmp1 tmp3
    --LEFT OUTER JOIN party_pkg.sls_svc_cust bcu ON tmp3.bilt_src_cust_id = bcu.src_cust_id    AND tmp3.cnsld_src_txn_bu_id = bcu.src_bu_id
    LEFT OUTER JOIN 
        (SELECT    
                src_bu_id,
                src_cust_id,
                src_postal_addr_id,
                src_cntct_id,
                cust_nm,
                parnt_cust_nm,
                ctry_cd,
                lang_cd,
                frst_nm,
                last_nm,
                elec_addr,
				ph_area_cd||ph_nbr AS ph_nbr
             FROM party_pkg.sls_svc_cust_ph_cntct_bridge
             WHERE addr_cntct_role_cd = 'BILL_TO' AND elec_addr IS NOT NULL     
             GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12)  bc     
    ON tmp3.cnsld_src_txn_bu_id = bc.src_bu_id AND tmp3.bilt_src_cust_id = bc.src_cust_id AND tmp3.bilt_addr_seq_id = bc.src_postal_addr_id  AND tmp3.bilt_cntct_id = bc.src_cntct_id        
    LEFT OUTER JOIN party_pkg.sls_svc_cust scu ON tmp3.shpto_src_cust_id = scu.src_cust_id    AND tmp3.cnsld_src_txn_bu_id = scu.src_bu_id    
    LEFT OUTER JOIN 
        (SELECT    
                src_bu_id,
                src_cust_id,
                src_postal_addr_id,
                src_cntct_id,
                cust_nm,
                parnt_cust_nm,
                ctry_cd,
                lang_cd,
                frst_nm,
                last_nm,
                elec_addr,
                ph_area_cd||ph_nbr AS ph_nbr
             FROM party_pkg.sls_svc_cust_ph_cntct_bridge
             WHERE addr_cntct_role_cd = 'SHIP_TO' AND elec_addr IS NOT NULL     
             GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12)  sc     
    ON tmp3.cnsld_src_txn_bu_id = sc.src_bu_id AND tmp3.shpto_src_cust_id = sc.src_cust_id AND tmp3.shpto_addr_seq_id = sc.src_postal_addr_id  AND tmp3.shpto_cntct_id = sc.src_cntct_id
    INNER JOIN corp_drm_pkg.phys_geo_hier pgh ON tmp3.cust_bu_id = pgh.bu_id
GROUP BY 1,2
;

DELETE FROM tmp_work_db.ph_lkup WHERE ph_nbr IS NULL;

INSERT INTO tmp_work_db.ph_lkup
SELECT
        tmp3.cnsld_src_txn_id,
        tmp3.cnsld_src_txn_bu_id,
        MAX(CAST(TRIM(COALESCE(CASE WHEN ctm.media_type_code = 'PHW' THEN ctm.media_value ELSE NULL end, CASE WHEN ctms.media_type_code = 'PHW' THEN ctms.media_value ELSE NULL end)) AS VARCHAR(254))) AS work_phone
FROM sas_pulse_stg.ev_gbl_cons_nps_tmp3 tmp3
LEFT OUTER JOIN finance.contact c 
        ON tmp3.bilt_cust_nbr = c.customer_num 
        AND tmp3.so_bu_id = c.business_unit_id
        AND tmp3.bilt_addr_seq_id = c.address_seq_num
        AND c.address_type_code = 'B'
LEFT OUTER JOIN finance.contact_media ctm
        ON c.customer_num = ctm.customer_num
        AND c.business_unit_id = ctm.business_unit_id
        AND c.contact_id = ctm.contact_id
        AND ctm.media_type_code IN ('PHW','EML','E')
LEFT OUTER JOIN finance.postal_address pa
        ON tmp3.bilt_cust_nbr = pa.customer_num 
        AND tmp3.bilt_addr_seq_id = pa.address_seq_num 
        AND tmp3.so_bu_id = pa.business_unit_id 
        AND pa.address_type_code = 'B'
LEFT OUTER JOIN finance.contact sc 
        ON tmp3.bilt_cust_nbr = sc.customer_num 
        AND tmp3.shpto_cust_nbr = sc.customer_num
        AND tmp3.so_bu_id = sc.business_unit_id
        AND tmp3.shpto_addr_seq_id = sc.address_seq_num
        AND sc.address_type_code = 'S'
LEFT OUTER JOIN finance.contact_media ctms
        ON sc.customer_num = ctms.customer_num
        AND sc.business_unit_id = ctms.business_unit_id
        AND sc.contact_id = ctms.contact_id
        AND ctms.media_type_code IN ('PHW','EML','E')
LEFT OUTER JOIN finance.postal_address pas
        ON tmp3.bilt_cust_nbr = pas.customer_num 
        AND tmp3.shpto_cust_nbr = pas.customer_num
        AND tmp3.shpto_addr_seq_id = pas.address_seq_num 
        AND tmp3.so_bu_id = pas.business_unit_id 
        AND pas.address_type_code = 'S'
INNER JOIN corp_drm_pkg.phys_geo_hier pgh ON tmp3.cust_bu_id = pgh.bu_id
WHERE pgh.rgn_abbr = 'AMER' AND so_bu_id = 11 AND SO_NBR NOT IN (SELECT cnsld_src_txn_id FROM tmp_work_db.ph_lkup)
GROUP BY 1,2
;

DELETE FROM tmp_work_db.ph_lkup WHERE ph_nbr IS NULL;




DELETE FROM sas_pulse_stg.ev_gbl_cons_nps_inc ALL;

COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp6 COLUMN EMAIL_ADDR_ID;
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp6 COLUMN SYS_COMB_HIER_CD;
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp6 COLUMN (CNSLD_SRC_TXN_BU_ID, PRIM_SLS_ASSOC_NBR);
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp6 COLUMN PRIM_SLS_ASSOC_NBR;
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp6 COLUMN (CUST_BU_ID, CUST_LCL_CHNL_CD);
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp6 COLUMN (CUST_BU_ID);

	 
--Delete sas_pulse_stg.ev_gbl_cons_nps_inc All 3541374 ,3537620 , 3399685 

INSERT INTO sas_pulse_stg.ev_gbl_cons_nps_inc
(
	evnt_id,
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
	sls_rep_bdge_nbr,
	sls_rep_email_addr_id,
	acct_exec_bdge_nbr,
	acct_exec_email_addr_id,
	insd_sls_mgr_bdge_nbr,
	insd_sls_mgr_email_addr_id,
	rgnl_sls_mgr_bdge_nbr,
	rgnl_sls_mgr_email_addr_id,
	sys_itm_cls_nm,
	sys_flg,
	snp_flg,
	svcs_flg,
	sls_chnl_cd,
	tot_revn_disc_amt,
	svc_req_nbr,
	svc_req_type,
	svc_req_crt_dt,
	svc_req_clos_dt,
	svc_req_crtd_by_bdge_nbr,
	svc_req_owner_bdge_nbr,
	svc_req_last_actvy_dt,
	svc_req_last_actvy_bdge_nbr,
	svc_tag_bu_id,
	svc_tag_lcl_chnl_cd,
	svc_tag_id,
	wrnty_type_cd,
	wrnty_stat_cd,
	dspch_nbr,
	dspch_type_cd,
	dspch_stat_cd,
	dspch_stat_dt,
	auth_guid_cd,
	top_lvl_acct_id,
	grp_acct_id,
	grp_co_nm,
	acct_id,
	acct_co_nm,
	acct_prim_cntct_nm,
	acct_owner_email_addr_id,
	cust_bu_id,
	cust_nbr,
	cust_lcl_chnl_cd,
	cust_co_nm,
	gbl_seg_desc,
	sub_seg_desc,
	sls_seg_desc,
	sls_vert_desc,
	ptnr_acct_type_cd,
	ptnr_nm,
	acct_rnkg_id,
	rad_cd,
	rad_nbr,
	gtm_gmm_nbr,
	iso_ctry_cd_2,
	iso_lang_cd,
	frst_nm,
	last_nm,
	full_nm,
	sfdc_cntct_id,
	email_addr_id,
	domain_email_addr_id,
	ph_nbr,
	job_ttl_nm,
	decsn_mkg_role_nm,
	prsn_grtg_txt,
	bnced_alert_flg,
	no_rsp_alert_flg,
	cntct_src_sys_nm,
	signatory_nm,
	signatory_ttl_nm,
	ce_email_addr_id,
	extrnl_src_txn_id,
	extrnl_src_attr_nm,
	extrnl_src_bu_id,
	attr_1_val,
	attr_2_val,
	attr_3_val,
	attr_4_val,
	attr_5_val,
	dw_iud_acct_nm,
	dw_ins_upd_dts,
	dw_ld_grp_val,
	dw_src_site_id
)
Select  (ROW_NUMBER() OVER (ORDER BY evnt_subtype_cd, cnsld_src_txn_id) + (SELECT MAX(evnt_id) FROM ce_base.survey_evnt WHERE evnt_ldr_id = 320)) AS evnt_id,
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
	sls_rep_bdge_nbr,
	sls_rep_email_addr_id,
	acct_exec_bdge_nbr,
	acct_exec_email_addr_id,
	insd_sls_mgr_bdge_nbr,
	insd_sls_mgr_email_addr_id,
	rgnl_sls_mgr_bdge_nbr,
	rgnl_sls_mgr_email_addr_id,
	sys_itm_cls_nm,
	sys_flg,
	snp_flg,
	svcs_flg,
	sls_chnl_cd,
	tot_revn_disc_amt,
	svc_req_nbr,
	svc_req_type,
	svc_req_crt_dt,
	svc_req_clos_dt,
	svc_req_crtd_by_bdge_nbr,
	svc_req_owner_bdge_nbr,
	svc_req_last_actvy_dt,
	svc_req_last_actvy_bdge_nbr,
	svc_tag_bu_id,
	svc_tag_lcl_chnl_cd,
	svc_tag_id,
	wrnty_type_cd,
	wrnty_stat_cd,
	dspch_nbr,
	dspch_type_cd,
	dspch_stat_cd,
	dspch_stat_dt,
	auth_guid_cd,
	top_lvl_acct_id,
	grp_acct_id,
	grp_co_nm,
	acct_id,
	acct_co_nm,
	acct_prim_cntct_nm,
	acct_owner_email_addr_id,
	cust_bu_id,
	cust_nbr,
	cust_lcl_chnl_cd,
	cust_co_nm,
	gbl_seg_desc,
	sub_seg_desc,
	sls_seg_desc,
	sls_vert_desc,
	ptnr_acct_type_cd,
	ptnr_nm,
	acct_rnkg_id,
	rad_cd,
	rad_nbr,
	gtm_gmm_nbr,
	iso_ctry_cd_2,
	iso_lang_cd,
	frst_nm,
	last_nm,
	full_nm,
	sfdc_cntct_id,
	email_addr_id,
	domain_email_addr_id,
	ph_nbr,
	job_ttl_nm,
	decsn_mkg_role_nm,
	prsn_grtg_txt,
	bnced_alert_flg,
	no_rsp_alert_flg,
	cntct_src_sys_nm,
	signatory_nm,
	signatory_ttl_nm,
	ce_email_addr_id,
	extrnl_src_txn_id,
	extrnl_src_attr_nm,
	extrnl_src_bu_id,
	attr_1_val,
	attr_2_val,
	attr_3_val,
	attr_4_val,
	attr_5_val,
	dw_iud_acct_nm,
	dw_ins_upd_dts,
	dw_ld_grp_val,
	dw_src_site_id
From (
SELECT	
	DISTINCT ---(ROW_NUMBER() OVER (ORDER BY evnt_subtype_cd, cnsld_src_txn_id) + (SELECT MAX(evnt_id) FROM ce_base.survey_evnt WHERE evnt_ldr_id = 320)) AS evnt_id,
	tmp6.evnt_ldr_id,
	tmp6.evnt_type_cd,
	tmp6.evnt_subtype_cd,
	tmp6.evnt_dt,
	tmp6.evnt_dts,
	tmp6.cnsld_src_txn_id,
	tmp6.cnsld_src_txn_bu_id,
	tmp6.so_nbr,
	tmp6.so_bu_id,
	tmp6.so_lcl_chnl_cd,
	tmp6.so_dt,
	tmp6.so_inv_dt,
	tmp6.qte_nbr,
	tmp6.qte_bu_id,
	tmp6.qte_lcl_chnl_cd,
	tmp6.qte_dt,
	CASE WHEN pgh.rgn_abbr IN ('APJ','EMEA') THEN aa.assoc_bdge_nbr
         WHEN tmp6.cnsld_src_txn_bu_id = 11 THEN ad.assoc_bdge_nbr
         WHEN tmp6.cnsld_src_txn_bu_id = 707 THEN ad3.assoc_bdge_nbr
         WHEN pgh.rgn_abbr = 'AMER' THEN COALESCE(ad2.assoc_bdge_nbr, ad.assoc_bdge_nbr)
         ELSE NULL
	END AS sls_rep_bdge_nbr,
	NULL AS sls_rep_email_addr_id,
	NULL AS acct_exec_bdge_nbr,
	NULL AS acct_exec_email_addr_id,
	NULL AS insd_sls_mgr_bdge_nbr,
	NULL AS insd_sls_mgr_email_addr_id,
	NULL AS rgnl_sls_mgr_bdge_nbr,
	NULL AS rgnl_sls_mgr_email_addr_id,
	tmp6.sys_itm_cls_nm,
	tmp6.sys_flg,
	tmp6.snp_flg,
	tmp6.svcs_flg,
	tmp6.sls_chnl_cd,
	tmp6.tot_revn_disc_amt,
	NULL AS svc_req_nbr,
	NULL AS svc_req_type,
	NULL AS svc_req_crt_dt,
	NULL AS svc_req_clos_dt,
	NULL AS svc_req_crtd_by_bdge_nbr,
	NULL AS svc_req_owner_bdge_nbr,
	NULL AS svc_req_last_actvy_dt,
	NULL AS svc_req_last_actvy_bdge_nbr,
	NULL AS svc_tag_bu_id,
	NULL AS svc_tag_lcl_chnl_cd,
	NULL AS svc_tag_id,
	NULL AS wrnty_type_cd,
	NULL AS wrnty_stat_cd,
	NULL AS dspch_nbr,
	NULL AS dspch_type_cd,
	NULL AS dspch_stat_cd,
	NULL AS dspch_stat_dt,
	NULL AS auth_guid_cd,
	NULL AS top_lvl_acct_id,
	NULL AS grp_acct_id,
	NULL AS grp_co_nm,
	tmp6.acct_id,
	tmp6.acct_co_nm,
	NULL AS acct_prim_cntct_nm,
	NULL AS acct_owner_email_addr_id,
	tmp6.cust_bu_id,
	tmp6.cust_nbr,
	tmp6.cust_lcl_chnl_cd,
	tmp6.cust_co_nm,
	NULL AS gbl_seg_desc,
	NULL AS sub_seg_desc,
	NULL AS sls_seg_desc,
	NULL AS sls_vert_desc,
	NULL AS ptnr_acct_type_cd,
	NULL AS ptnr_nm,
	NULL AS acct_rnkg_id,
	NULL AS rad_cd,
	NULL AS rad_nbr,
	NULL AS gtm_gmm_nbr,
	COALESCE(tmp6.iso_ctry_cd_2, cll.iso_ctry_2_cd) AS iso_ctry_cd_2,
	COALESCE(tmp6.iso_lang_cd, cll.iso_lang_cd) AS iso_lang_cd,
	tmp6.frst_nm,
	tmp6.last_nm,
	tmp6.full_nm,
	NULL AS sfdc_cntct_id,
	tmp6.email_addr_id,
	SUBSTR(email_addr_id, CASE WHEN POSITION('@'IN email_addr_id) = 0 THEN NULL ELSE POSITION('@' IN email_addr_id) END) AS domain_email_addr_id,
	NULL AS ph_nbr,
	NULL AS job_ttl_nm,
	NULL AS decsn_mkg_role_nm,
	NULL AS prsn_grtg_txt,
	NULL AS bnced_alert_flg,
	NULL AS no_rsp_alert_flg,
	NULL AS cntct_src_sys_nm,
	NULL AS signatory_nm,
	NULL AS signatory_ttl_nm,
	/*tmp6.SYS_COMB_HIER_CD AS signatory_nm,
	tmp6.SNP_COMB_HIER_CD AS signatory_ttl_nm,*/
	NULL AS ce_email_addr_id,
	NULL AS extrnl_src_txn_id,
	NULL AS extrnl_src_attr_nm,
	NULL AS extrnl_src_bu_id,
	CASE WHEN (SELECT MAX(evnt_dt) FROM sas_pulse_stg.ev_gbl_cons_nps_tmp6) - tmp6.evnt_dt < 180 THEN  '0-6 Months' 
		WHEN (SELECT MAX(evnt_dt) FROM sas_pulse_stg.ev_gbl_cons_nps_tmp6) - tmp6.evnt_dt BETWEEN 180 AND 367 THEN  '7-12 Months'
		 ELSE '13-18 Months'END AS attr_1_val,
	tmp6.SYS_COMB_HIER_CD AS attr_2_val,
	tmp6.SNP_COMB_HIER_CD AS attr_3_val,
	/*crc.CONSUMER_REGION AS attr_2_val,
	CASE WHEN pf.PRODUCT_FRANCHISE IS NOT NULL THEN pf.PRODUCT_FRANCHISE ELSE 'UNKNOWN' END AS attr_3_val,*/
	tmp6.prim_sls_assoc_nbr AS attr_4_val,
	NULL AS attr_5_val,
	'SERVICE_RPE_PULSE_ETL'  AS dw_iud_acct_nm,
	CURRENT_TIMESTAMP AS dw_ins_upd_dts,
	CAST(CAST(CURRENT_DATE AS CHAR(10)) || CAST(CURRENT_TIME AS CHAR(8)) AS DECIMAL(18)) AS dw_ld_grp_val,
	82 AS dw_src_site_id
FROM sas_pulse_stg.ev_gbl_cons_nps_tmp6 tmp6
LEFT OUTER JOIN ce_base.survey_ctry_lang_lkup cll
	ON tmp6.cust_bu_id = cll.bu_id
INNER JOIN corp_drm_pkg.phys_geo_hier pgh ON tmp6.cust_bu_id = pgh.bu_id
/*INNER JOIN sas_pulse_stg.CONS_REGIONAL_CHANNELS crc
	ON tmp6.cust_bu_id = crc.BUSINESS_UNIT_ID AND tmp6.cust_lcl_chnl_cd = crc.LOCAL_CHANNEL
LEFT OUTER JOIN itm_pkg.comb_prod_hier cph 		
	ON	tmp6.SYS_COMB_HIER_CD = cph.comb_hier_cd AND cph.HIER_TYPE_DESC = 'PROD HIER'
left outer join (SELECT MAX(SVC_TAG_ID) AS SVC_TAG_ID, ORD_NBR, ORD_BU_ID FROM svc_base.cust_prod GROUP BY 2,3) cp ON tmp6.so_bu_id = cp.ord_bu_id and tmp6.so_nbr = cp.ord_nbr 							
LEFT OUTER JOIN sas_pulse_stg.PROD_FRANCHISE pf
	ON cph.PROD_ROLLUP_CD = pf.PRODUCT_LINE*/
LEFT OUTER JOIN party_pkg.agnt_assoc aa --emea and ap (partial india)
   ON tmp6.prim_sls_assoc_nbr = aa.agnt_assoc_nbr
   AND tmp6.cnsld_src_txn_bu_id = aa.bu_id
   And AGNT_ASSOC_END_DT = '9999-12-31' ---Added by EL 
LEFT OUTER JOIN party_pkg.assoc_dim ad -- us and some lata
   ON tmp6.prim_sls_assoc_nbr = TRIM(TRIM(TRAILING '.' FROM CAST(ad.doms_login_id AS VARCHAR(12))))
   AND ad.src_eff_end_dt = '9999-12-31'
LEFT OUTER JOIN party_pkg.assoc_dim ad2 -- lata
   ON tmp6.prim_sls_assoc_nbr = TRIM(TRIM(TRAILING '.' FROM CAST(ad2.doms_login2 AS VARCHAR(12))))
   AND ad2.src_eff_end_dt = '9999-12-31'
LEFT OUTER JOIN party_pkg.assoc_dim ad3 -- can
   ON tmp6.prim_sls_assoc_nbr = TRIM(TRIM(TRAILING '.' FROM CAST(ad3.doms_login3 AS VARCHAR(12))))
   AND ad3.src_eff_end_dt = '9999-12-31'
WHERE tmp6.email_addr_id IS NOT NULL
	AND tmp6.email_addr_id LIKE '%@%.%'
	AND tmp6.email_addr_id NOT LIKE '%@dell.com%'
	AND tmp6.email_addr_id NOT IN (SELECT email_addr_id FROM ce_base.SURVEY_EMAIL_XCLSN_LKUP)
	---and sls_rep_bdge_nbr <> '-1'
) A ;


UPDATE INC
FROM sas_pulse_stg.ev_gbl_cons_nps_inc INC, tmp_work_db.ph_lkup PH
SET PH_NBR = PH.PH_NBR
WHERE INC.cnsld_src_txn_id=PH.cnsld_src_txn_id AND INC.cnsld_src_txn_bu_id = PH.cnsld_src_txn_bu_id
;



--00:00:54	3035427


/*
1st Try : 
INSERT   rows in 01:47---Excluded sls_rep_bdge_nbr -1  with unique records
----3541374 rows in 01:57 --Included sls_rep_bdge_nbr -1
4,110,196 rows in 00:56
7652907 rows in 00:01:07

2nd Try : 
--3399685  rows 02:24 

   LEFT OUTER JOIN party_pkg.agnt_assoc aa --emea and ap (partial india)
   ON tmp6.prim_sls_assoc_nbr = aa.agnt_assoc_nbr
   AND tmp6.cnsld_src_txn_bu_id = aa.bu_id
   And AGNT_ASSOC_END_DT = '9999-12-31' ---Added by EL 


---delete SAS_PULSE_STG.ev_gbl_extrnl all;
---930907
Select Count(1) From SAS_PULSE_STG.ev_gbl_extrnl

INSERT INTO SAS_PULSE_STG.ev_gbl_extrnl
(
	evnt_ldr_Id,
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
	sls_rep_bdge_nbr,
	sls_rep_email_addr_id,
	acct_exec_bdge_nbr,
	acct_exec_email_addr_id,
	insd_sls_mgr_bdge_nbr,
	insd_sls_mgr_email_addr_id,
	rgnl_sls_mgr_bdge_nbr,
	rgnl_sls_mgr_email_addr_id,
	sys_itm_cls_nm,
	sys_flg,
	snp_flg,
	svcs_flg,
	sls_chnl_cd,
	tot_revn_disc_amt,
	svc_req_nbr,
	svc_req_type,
	svc_req_crt_dt,
	svc_req_clos_dt,
	svc_req_crtd_by_bdge_nbr,
	svc_req_owner_bdge_nbr,
	svc_req_last_actvy_dt,
	svc_req_last_actvy_bdge_nbr,
	svc_tag_bu_id,
	svc_tag_lcl_chnl_cd,
	svc_tag_id,
	wrnty_type_cd,
	wrnty_stat_cd,
	dspch_nbr,
	dspch_type_cd,
	dspch_stat_cd,
	dspch_stat_dt,
	auth_guid_cd,
	top_lvl_acct_id,
	grp_acct_id,
	grp_co_nm,
	acct_id,
	acct_co_nm,
	acct_prim_cntct_nm,
	acct_owner_email_addr_id,
	cust_bu_id,
	cust_nbr,
	cust_lcl_chnl_cd,
	cust_co_nm,
	gbl_seg_desc,
	sub_seg_desc,
	sls_seg_desc,
	sls_vert_desc,
	ptnr_acct_type_cd,
	ptnr_nm,
	acct_rnkg_id,
	rad_cd,
	rad_nbr,
	gtm_gmm_nbr,
	iso_ctry_2_cd,
	iso_lang_cd,
	frst_nm,
	last_nm,
	full_nm,
	sfdc_cntct_id,
	email_addr_id,
	domain_email_addr_id,
	ph_nbr,
	job_ttl_nm,
	decsn_mkg_role_nm,
	prsn_grtg_txt,
	bnced_alert_flg,
	no_rsp_alert_flg,
	cntct_src_sys_nm,
	signatory_nm,
	signatory_ttl_nm,
	ce_email_addr_id,
	extrnl_src_txn_id,
	extrnl_src_attr_nm,
	extrnl_src_bu_id,
	attr_1_val,
	attr_2_val,
	attr_3_val,
	attr_4_val,
	attr_5_val
)
SELECT	DISTINCT 
	evnt_ldr_Id,
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
	sls_rep_bdge_nbr,
	sls_rep_email_addr_id,
	acct_exec_bdge_nbr,
	acct_exec_email_addr_id,
	insd_sls_mgr_bdge_nbr,
	insd_sls_mgr_email_addr_id,
	rgnl_sls_mgr_bdge_nbr,
	rgnl_sls_mgr_email_addr_id,
	sys_itm_cls_nm,
	sys_flg,
	snp_flg,
	svcs_flg,
	sls_chnl_cd,
	tot_revn_disc_amt,
	svc_req_nbr,
	svc_req_type,
	svc_req_crt_dt,
	svc_req_clos_dt,
	svc_req_crtd_by_bdge_nbr,
	svc_req_owner_bdge_nbr,
	svc_req_last_actvy_dt,
	svc_req_last_actvy_bdge_nbr,
	svc_tag_bu_id,
	svc_tag_lcl_chnl_cd,
	svc_tag_id,
	wrnty_type_cd,
	wrnty_stat_cd,
	dspch_nbr,
	dspch_type_cd,
	dspch_stat_cd,
	dspch_stat_dt,
	auth_guid_cd,
	top_lvl_acct_id,
	grp_acct_id,
	grp_co_nm,
	acct_id,
	acct_co_nm,
	acct_prim_cntct_nm,
	acct_owner_email_addr_id,
	cust_bu_id,
	cust_nbr,
	cust_lcl_chnl_cd,
	cust_co_nm,
	gbl_seg_desc,
	sub_seg_desc,
	sls_seg_desc,
	sls_vert_desc,
	ptnr_acct_type_cd,
	ptnr_nm,
	acct_rnkg_id,
	rad_cd,
	rad_nbr,
	gtm_gmm_nbr,
	ISO_CTRY_CD_2,
	iso_lang_cd,
	frst_nm,
	last_nm,
	full_nm,
	sfdc_cntct_id,
	email_addr_id,
	domain_email_addr_id,
	ph_nbr,
	job_ttl_nm,
	decsn_mkg_role_nm,
	prsn_grtg_txt,
	bnced_alert_flg,
	no_rsp_alert_flg,
	cntct_src_sys_nm,
	signatory_nm,
	signatory_ttl_nm,
	ce_email_addr_id,
	cnsld_src_txn_id AS extrnl_src_txn_id,
	extrnl_src_attr_nm,
	cnsld_src_txn_bu_id As extrnl_src_bu_id,
	attr_1_val,
	attr_2_val,
	attr_3_val,
	attr_4_val,
	attr_5_val
From sas_pulse_stg.ev_gbl_cons_nps_inc


-- Checking PI 
Select EVNT_SUBTYPE_CD ,cnsld_src_txn_id ,cnsld_src_txn_bu_id,
-- EXTRNL_SRC_TXN_ID ,EXTRNL_SRC_BU_ID,
count(1) 
From sas_pulse_stg.ev_gbl_cons_nps_inc
Where SLS_REP_BDGE_NBR <> '-1'
Group by 1,2,3
having count(1) > 1

--****** Insert into ce_base stg table. Insert will be trigger by AP job ******
Insert Into SAS_PULSE_STG.ev_gbl_extrnl
Select * From sas_pulse_stg.ev_gbl_cons_nps_inc;


Select Distinct *From sas_pulse_stg.ev_gbl_cons_nps_inc Where CNSLD_SRC_TXN_ID = '679857405'
show table SAS_PULSE_STG.ev_gbl_extrnl

Select Top 1 * From  sas_pulse_stg.ev_gbl_cons_nps_inc ;
Select Top 1 * From  sas_pulse_stg.ev_gbl_extrnl ;
*/
