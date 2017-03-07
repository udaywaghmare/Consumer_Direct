CREATE SET TABLE tmp_work_db.FY16H2_NPS_AUDIENCE_6 ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      EVNT_ID DECIMAL(18,0),
      PULSE_ADNC_SELTN_ID DECIMAL(18,0),
      CONTACT_NAME VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
      EMAIL_ADDRESS VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      PHONE_NUMBER VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
      "LANGUAGE" VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SURVEY_CLOSE_DATE VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      SUPPORT_LINK VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
      PRIVACY_LINK VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ACCOUNT_ID VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      SURVEY VARCHAR(15) CHARACTER SET LATIN CASESPECIFIC,
      WAVE VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      OVERSAMPLE_FLAG VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      PURCHASE_RECENCY VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      ORDER_NUMBER VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      SVC_TAG VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ORDER_DATE DATE FORMAT 'YYYY-MM-DD',
      CUSTOMER_NUMBER VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      CONTACT_ID VARCHAR(15) CHARACTER SET LATIN CASESPECIFIC,
      DECISION_MAKING_ROLE VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SALUTATION VARCHAR(300) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SIGNATORY_NAME_1 VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SIGNATORY_TITLE_1 VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SIGNATORY_NAME_2 VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SIGNATORY_TITLE_2 VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
      LETTER_CUST_TYPE VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      SERVICES_CONTACT_FLAG VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      SOFTWARE_CONTACT_FLAG VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      SFDC_TASK_LINK VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SR_EMAIL VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      AE_EMAIL VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      ISM_EMAIL VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      RSM_EMAIL VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      GAM_EMAIL VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      TAM_EMAIL VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      CAM_EMAIL VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      PDM_EMAIL VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      GCP_PM_EMAIL VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      OEM_SE_EMAIL VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      OEM_PM_EMAIL VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SERVICES_PPOC_EMAIL VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SERVICES_DELEGATE_EMAIL VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SERVICES_OPS_LEAD_EMAIL VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SERVICES_MAILBOX_EMAIL VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      CX_EMAIL VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC,
      DET_TASK_OWNER_ID VARCHAR(12) CHARACTER SET LATIN NOT CASESPECIFIC,
      PAS_TASK_OWNER_ID VARCHAR(12) CHARACTER SET LATIN NOT CASESPECIFIC,
      PRO_TASK_OWNER_ID VARCHAR(12) CHARACTER SET LATIN NOT CASESPECIFIC,
      SUB_ACCOUNT_ID DECIMAL(15,0),
      SUB_ACCOUNT_NAME VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
      JOB_TITLE VARCHAR(500) CHARACTER SET UNICODE NOT CASESPECIFIC,
      TRANS_SALES_CHANNEL VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      PRODUCT_LVL1 VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
      PRODUCT_LVL2 VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
      PRODUCT_LVL3 VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
      PRODUCT_LVL4 VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
      PRODUCT_LVL5 VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC)
PRIMARY INDEX ( PULSE_ADNC_SELTN_ID );

insert into sas_pulse_stg.ev_gbl_cons_nps_inc_tmp
SELECT 		
	tmp.*
FROM sas_pulse_stg.ev_gbl_cons_nps_inc tmp 		
INNER JOIN corp_drm_pkg.PHYS_GEO_HIER pgh ON tmp.cust_bu_id = pgh.bu_id 		
INNER JOIN corp_drm_pkg.chnl_hier ch ON tmp.cust_bu_id = ch.BU_ID AND tmp.cust_lcl_chnl_cd = ch.LCL_CHNL_CODE 		
INNER join itm_pkg.comb_prod_hier prod on coalesce(tmp.attr_2_val, tmp.attr_3_val) = prod.comb_hier_cd		
WHERE tmp.email_addr_id NOT IN (SELECT TRIM(es.email_address) FROM marcom.email_subscriber es WHERE es.emailable_flag = 'N')		
	AND tmp.email_addr_id NOT IN (SELECT EMAIL_ADDR_ID FROM SAS_PULSE_STG.survey_invite WHERE WAVE_DESC = 'FY16Q2' OR WAVE_DESC = 'FY16Q3') 	
	AND tmp.email_addr_id NOT IN (SELECT EMAIL_ADDR_ID FROM ce_base.survey_evnt se INNER JOIN ce_base.survey_adnc_seltn_log asl ON se.EVNT_ID = asl.EVNT_ID	WHERE asl.adnc_run_dt > CURRENT_DATE - 30 AND dstrb_stat_cd = 'Complete') 
	AND tmp.email_addr_id NOT IN (SELECT EMAIL_ADDR_ID FROM CE_BASE.SURVEY_DO_NOT_DSTRB_LIST) 	
	AND tmp.domain_email_addr_id NOT IN (SELECT EMAIL_ADDR_ID FROM CE_BASE.SURVEY_DO_NOT_DSTRB_LIST) 	
	AND prod.prod_grp_desc in ('Consumer','CS SOFTWARE AND PERIPHERALS','Tablet Devices')	
	AND NOT (ch.bu_id = 3535 and ch.lcl_chnl_code = '35034')	
	AND tmp.iso_ctry_cd_2 not in ('US','CA');
	

insert into SAS_PULSE_STG.b2c_fy16h2_ev_gbl_extrnl
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
SELECT
	evnt_ldr_Id,
	'B2CNPS' as evnt_type_cd,
	'DIRECT' as evnt_subtype_cd,
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
	'FY16 H2' as attr_5_val
from sas_pulse_stg.ev_gbl_cons_nps_inc_tmp
sample
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'BR' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 4516
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'BR' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 1342
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'BR' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 174
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'BR' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 257
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'BR' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 4293
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'BR' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 1510
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'BR' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 320
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'BR' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 239
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'MX' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 178
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'MX' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 268
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'MX' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 89
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'MX' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 16
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'MX' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 632
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'MX' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 340
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'MX' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 64
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'MX' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 135
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'GB' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 6112
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'GB' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 253.5
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'GB' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 343
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'GB' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'GB' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 4950
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'GB' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 409
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'GB' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 376
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'GB' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'DE' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 3568
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'DE' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'DE' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 257
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'DE' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'DE' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 3626
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'DE' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'DE' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 207
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'DE' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'FR' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 3589
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'FR' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'FR' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 395
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'FR' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'FR' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 2440
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'FR' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'FR' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 353
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'FR' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'JP' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 7661
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'JP' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 1096
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'JP' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 220
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'JP' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 45
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'JP' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 5264
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'JP' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 796
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'JP' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 315
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'JP' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 36
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'CN' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 325
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'CN' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 2524
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'CN' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 0
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'CN' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 352
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'CN' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 229
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'CN' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 2318
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'CN' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 0
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'CN' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 282
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'AU' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 1954
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'AU' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 708
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'AU' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 380
	when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'AU' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 66
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'AU' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 2198
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'AU' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 636
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'AU' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 378
	when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'AU' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 87
end	

insert into SAS_PULSE_STG.RTL_END_USERS_tmp
select * 
FROM  SAS_PULSE_STG.RTL_END_USERS_extrnl tmp 		
WHERE tmp.email_addr_id NOT IN (SELECT TRIM(es.email_address) FROM marcom.email_subscriber es WHERE es.emailable_flag = 'N')		
	AND tmp.email_addr_id NOT IN (SELECT EMAIL_ADDR_ID FROM SAS_PULSE_STG.survey_invite WHERE WAVE_DESC = 'FY16Q2' OR WAVE_DESC = 'FY16Q3') 	
	AND tmp.email_addr_id NOT IN (SELECT EMAIL_ADDR_ID FROM ce_base.survey_evnt se INNER JOIN ce_base.survey_adnc_seltn_log asl ON se.EVNT_ID = asl.EVNT_ID	WHERE asl.adnc_run_dt > CURRENT_DATE - 30 AND dstrb_stat_cd = 'Complete') 
	AND tmp.email_addr_id NOT IN (SELECT EMAIL_ADDR_ID FROM CE_BASE.SURVEY_DO_NOT_DSTRB_LIST) 	
	AND tmp.domain_email_addr_id NOT IN (SELECT EMAIL_ADDR_ID FROM CE_BASE.SURVEY_DO_NOT_DSTRB_LIST) 	
	and iso_ctry_2_cd <> 'US' ;
	
	
insert into SAS_PULSE_STG.b2c_fy16h2_ev_gbl_extrnl
SELECT * FROM  SAS_PULSE_STG.RTL_END_USERS_tmp tmp 	
sample
	when attr_1_val = '0-6 Months' and iso_ctry_2_cd = 'BR' then 9558
	when attr_1_val = '7-12 Months' and iso_ctry_2_cd = 'BR' then 9814
	when attr_1_val = '0-6 Months' and iso_ctry_2_cd = 'MX' then 4994
	when attr_1_val = '7-12 Months' and iso_ctry_2_cd = 'MX' then 4516
	when attr_1_val = '0-6 Months' and iso_ctry_2_cd = 'CN' then 12334
	when attr_1_val = '7-12 Months' and iso_ctry_2_cd = 'CN' then 8378
	when attr_1_val = '0-6 Months' and iso_ctry_2_cd = 'AU' then 8149
	when attr_1_val = '7-12 Months' and iso_ctry_2_cd = 'AU' then 6247
	when attr_1_val = '0-6 Months' and iso_ctry_2_cd = 'IN' and attr_4_val is null or attr_4_val <> 'Tally' then 2657
	when attr_1_val = '0-6 Months' and iso_ctry_2_cd = 'IN' and attr_4_val = 'Tally' then 5489
	when attr_1_val = '7-12 Months' and iso_ctry_2_cd = 'IN' and attr_4_val is null or attr_4_val <> 'Tally' then 2683
	when attr_1_val = '7-12 Months' and iso_ctry_2_cd = 'IN' and attr_4_val = 'Tally'  then 4471
end

delete from SAS_PULSE_STG.b2c_fy16h2_ev_gbl_extrnl
where  iso_ctry_2_cd = 'IN' and evnt_subtype_cd = 'RETAIL' and attr_4_val <> 'Tally';

insert into SAS_PULSE_STG.b2c_fy16h2_ev_gbl_extrnl
SELECT * FROM  SAS_PULSE_STG.RTL_END_USERS_tmp tmp 	
where iso_ctry_2_cd = 'IN' and evnt_subtype_cd = 'RETAIL' and attr_4_val <> 'Tally';

select max(adnc_seltn_id) from SAS_PULSE_STG.b2c_fy16h2_adnc_seltn_ids;

select * 
from SAS_PULSE_STG.b2c_fy16h2_ev_gbl_extrnl ev
inner join SAS_PULSE_STG.b2c_fy16h2_adnc_seltn_ids a on ev.cnsld_src_txn_id = a.cnsld_src_txn_id
and ev.cnsld_src_txn_bu_id = a.cnsld_src_txn_bu_id
where ev.iso_ctry_2_cd not in ('US','CA')

insert into SAS_PULSE_STG.b2c_fy16h2_adnc_seltn_ids
select
	(ROW_NUMBER() OVER (ORDER BY cnsld_src_txn_id, cnsld_src_txn_bu_id) + 810175085),
	 cnsld_src_txn_id, cnsld_src_txn_bu_id
from SAS_PULSE_STG.b2c_fy16h2_ev_gbl_extrnl
where iso_ctry_2_cd not in ('US','CA');



INSERT INTO tmp_work_db.FY16H2_NPS_AUDIENCE_6
SEL 
NULL        AS          EVNT_ID              ,
a.adnc_seltn_id     AS          PULSE_ADNC_SELTN_ID               ,
ev.FULL_NM           AS          CONTACT_NAME             ,
ev.EMAIL_ADDR_ID             AS          EMAIL_ADDRESS              ,
td_sysfnlib.oreplace(td_sysfnlib.oreplace(ev.PH_NBR,'09'XC,' '),'0D0A'XC,' ')      AS          PHONE_NUMBER                ,
lnk_lkup.enum_lang     AS          'LANGUAGE'n   ,
lnk_lkup.Survey_Close_DT          AS          SURVEY_CLOSE_DATE   ,
CASE WHEN direct_flg = 'x' THEN lnk_lkup.supp_link  ELSE NULL END   AS          SUPPORT_LINK ,
CASE WHEN direct_flg = 'x' THEN lnk_lkup.prv_link  ELSE NULL END       AS          PRIVACY_LINK  ,
TRIM(TRAILING '.' FROM CAST(ev.CUST_BU_ID AS VARCHAR(20))) || '-' || TRIM(ev.cust_lcl_chnl_cd)   AS          ACCOUNT_ID                ,
'B2C NPS'            AS          SURVEY                ,
'FY16 H2'             AS          WAVE   ,
'No'       AS          OVERSAMPLE_FLAG       ,
ev.attr_1_val   AS          PURCHASE_RECENCY    ,
TRIM(TRAILING '.' FROM CAST(ev.SO_BU_ID AS  VARCHAR(20))) || '-' ||TRIM(ev.SO_NBR)            AS          ORDER_NUMBER             ,
TRIM(TRAILING '.' FROM CAST(ev.SVC_TAG_BU_ID AS  VARCHAR(20))) || '-' ||TRIM(ev.SVC_TAG_ID)       AS          SVC_TAG             ,
CAST(CAST(TRIM(SUBSTR(coalesce(ev.SO_DT,ev.evnt_dt),1,10))               AS  DATE FORMAT'YYYY-MM-DD')         AS VARCHAR(10)) AS  ORDER_DATE     ,
TRIM(TRAILING '.' FROM CAST(ev.CUST_BU_ID AS VARCHAR(20))) || '-' ||TRIM(ev.CUST_NBR)       AS          CUSTOMER_NUMBER   ,
NULL     AS          CONTACT_ID     ,
'Unknown/Blank'            AS          DECISION_MAKING_ROLE           ,
NULL   AS          SALUTATION      ,
NULL       AS          SIGNATORY_NAME_1   ,
NULL         AS          SIGNATORY_TITLE_1      ,
NULL     AS          SIGNATORY_NAME_2   ,
NULL     AS          SIGNATORY_TITLE_2      ,
NULL          AS          LETTER_CUST_TYPE        ,
'No'       AS          SERVICES_CONTACT_FLAG         ,
'No'       AS          SOFTWARE_CONTACT_FLAG     ,
NULL     AS          SFDC_TASK_LINK             ,
NULL     AS          SR_EMAIL           ,
NULL     AS          AE_EMAIL           ,
NULL     AS          ISM_EMAIL        ,
NULL     AS          RSM_EMAIL       ,
NULL     AS          GAM_EMAIL      ,
NULL     AS          TAM_EMAIL       ,
NULL     AS          CAM_EMAIL      ,
NULL     AS          PDM_EMAIL      ,
NULL     AS          GCP_PM_EMAIL              ,
NULL     AS          OEM_SE_EMAIL               ,
NULL     AS          OEM_PM_EMAIL             ,
NULL     AS          SERVICES_PPOC_EMAIL               ,
NULL     AS          SERVICES_DELEGATE_EMAIL      ,
NULL     AS          SERVICES_OPS_LEAD_EMAIL     ,
NULL     AS          SERVICES_MAILBOX_EMAIL       ,
NULL     AS          CX_EMAIL           ,
NULL     AS          DET_TASK_OWNER_ID ,
NULL     AS          PAS_TASK_OWNER_ID ,
NULL     AS          PRO_TASK_OWNER_ID ,
TRIM(TRAILING '.' FROM CAST(ev.CUST_BU_ID AS VARCHAR(20))) || '-' ||TRIM(ev.CUST_NBR)      AS          SUB_ACCOUNT_ID         ,
ev.cust_co_nm     AS          SUB_ACCOUNT_NAME ,
NULL     AS          JOB_TITLE           ,
CASE 
	WHEN ev.sls_chnl_cd = 'ONLINE' THEN 'Online' 
	WHEN ev.sls_chnl_cd = 'OFFLINE' THEN 'Offline' 
	WHEN ev.evnt_subtype_CD = 'RETAIL' then 'Retail' end           AS                 TRANS_SALES_CHANNEL             ,
case when prod.PROD_GRP_DESC in  ('Consumer','Tablet Devices','Thin Client') then 'Client Products'		
	when prod.PROD_GRP_DESC = 'CS SOFTWARE AND PERIPHERALS' then 'Client S&P'		
	else prod.PROD_GRP_DESC end as PRODUCT_LVL1,		
case when prod.LOB_DESC = 'CS 3RD PARTY SOFTWARE' then '3rd Party Software'			
	when prod.LOB_DESC = 'CLIENT PERIPHERALS' then		
		case when prod.BRAND_CATG_DESC = 'PARTNER' then '3rd Party Peripherals'	
			when prod.BRAND_CATG_DESC = 'DELL-BRANDED' then 'Dell-Branded Peripherals'
			else 'Peripherals' end
	when prod.LOB_DESC = 'DISPLAYS' then		
		case when prod.BRAND_CATG_DESC = 'PARTNER' then '3rd Party Displays'	
			when prod.BRAND_CATG_DESC = 'DELL-BRANDED' then 'Dell-Branded Displays'
			else 'Displays' end
	else prod.LOB_DESC end as PRODUCT_LVL2,		
case when prod.LOB_DESC in ('CS 3RD PARTY SOFTWARE',  'CLIENT PERIPHERALS', 'DISPLAYS') then prod.BASE_PROD_OFFRG_GRP_DESC			
	else prod.BRAND_CATG_DESC end as PRODUCT_LVL3,		
case when prod.LOB_DESC in ('CS 3RD PARTY SOFTWARE',  'CLIENT PERIPHERALS', 'DISPLAYS') then prod.PROD_OFFRG_DESC end as PRODUCT_LVL4,			
coalesce(ev.attr_2_val, ev.attr_3_val) as PRODUCT_LVL5			
FROM SAS_PULSE_STG.b2c_fy16h2_ev_gbl_extrnl ev
INNER JOIN SAS_PULSE_STG.b2c_fy16h2_adnc_seltn_ids a on ev.cnsld_src_txn_id = a.cnsld_src_txn_id and ev.cnsld_src_txn_bu_id = a.cnsld_src_txn_bu_id
LEFT OUTER JOIN sas_pulse_stg.comm_nps_link_lkup lnk_lkup ON ev.iso_ctry_2_cd = lnk_lkup.iso_ctry_cd AND ev.iso_lang_cd=lnk_lkup.ISO_LANG_CD
inner join itm_pkg.comb_prod_hier prod on coalesce(ev.attr_2_val, ev.attr_3_val)  = prod.comb_hier_cd	
where ev.iso_ctry_2_cd not in ('US','CA');

update tmp_work_db.FY16H2_NPS_AUDIENCE_6
set full_nm = trim(td_sysfnlib.oreplace(td_sysfnlib.oreplace(full_nm,'09'XC,' '),'0D0A'XC,' '));  

delete from  tmp_work_db.FY16H2_NPS_AUDIENCE_6 where product_lvl5 in ('DSE2716HN',
'DUZ2315HN',
'DSE2216HN',
'DP2314TN',
'DE2416HN',
'DSE2416HN'
);

insert into sas_pulse_stg.FY16H2_NPS_AUDIENCE
select
 ROW_NUMBER() OVER (ORDER BY PULSE_ADNC_SELTN_ID) + 1000000 AS EVNT_ID	,
PULSE_ADNC_SELTN_ID	,
CONTACT_NAME	,
EMAIL_ADDRESS	,
PHONE_NUMBER	,
"LANGUAGE"	,
SURVEY_CLOSE_DATE	,
SUPPORT_LINK	,
PRIVACY_LINK	,
ACCOUNT_ID	,
SURVEY	,
WAVE	,
OVERSAMPLE_FLAG	,
PURCHASE_RECENCY	,
ORDER_NUMBER	,
SVC_TAG	,
ORDER_DATE	,
CUSTOMER_NUMBER	,
CONTACT_ID	,
DECISION_MAKING_ROLE	,
SALUTATION	,
SIGNATORY_NAME_1	,
SIGNATORY_TITLE_1	,
SIGNATORY_NAME_2	,
SIGNATORY_TITLE_2	,
LETTER_CUST_TYPE	,
SERVICES_CONTACT_FLAG	,
SOFTWARE_CONTACT_FLAG	,
SFDC_TASK_LINK	,
SR_EMAIL	,
AE_EMAIL	,
ISM_EMAIL	,
RSM_EMAIL	,
GAM_EMAIL	,
TAM_EMAIL	,
CAM_EMAIL	,
PDM_EMAIL	,
GCP_PM_EMAIL	,
OEM_SE_EMAIL	,
OEM_PM_EMAIL	,
SERVICES_PPOC_EMAIL	,
SERVICES_DELEGATE_EMAIL	,
SERVICES_OPS_LEAD_EMAIL	,
SERVICES_MAILBOX_EMAIL	,
CX_EMAIL	,
DET_TASK_OWNER_ID	,
PAS_TASK_OWNER_ID	,
PRO_TASK_OWNER_ID	,
SUB_ACCOUNT_ID	,
SUB_ACCOUNT_NAME	,
JOB_TITLE	,
TRANS_SALES_CHANNEL	,
PRODUCT_LVL1	,
PRODUCT_LVL2	,
PRODUCT_LVL3	,
PRODUCT_LVL4	,
PRODUCT_LVL5	
from tmp_work_db.FY16H2_NPS_AUDIENCE_6;

CREATE SET TABLE tmp_work_db.b2b_acct_fy16h2_medallia_6 ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      ACCOUNT_ID VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      ACCOUNT_NAME VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
      CUSTOMER_REL_TYPE VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      ACCOUNT_TYPE VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      ACCOUNT_RANKING VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      SALES_SEG_LVL1 VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      SALES_SEG_LVL2 VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      SALES_SEG_LVL3 VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      SALES_SEG_LVL4 VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      SALES_SEG_LVL5 VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      SALES_SEG_LVL6 VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      GLOBAL_SEGMENT VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      WEIGHTING_ID INTEGER,
      REGION VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      SUB_REGION VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      COUNTRY VARCHAR(100) CHARACTER SET LATIN NOT CASESPECIFIC,
      RAD_CODE VARCHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      GLOBAL_ACCOUNT_ID VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      GLOBAL_ACCOUNT_NAME VARCHAR(360) CHARACTER SET UNICODE NOT CASESPECIFIC,
      GMM VARCHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      PARTNER_REL_TYPE VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      PARTNER_TYPE VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
      PARTNER_TIER VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      PARTNER_CERT VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
      SERVICES_INDUSTRY VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC)
UNIQUE PRIMARY INDEX ( ACCOUNT_ID );

insert into tmp_work_db.b2b_acct_fy16h2_medallia_6
select
	a.ACCOUNT_ID as ACCOUNT_ID,	
	ch.lcl_chnl_desc as ACCOUNT_NAME,	
	'Transactional' as CUSTOMER_REL_TYPE,
	null as ACCOUNT_TYPE,
	null as ACCOUNT_RANKING,
	case when pgh.ctry_desc in ('Brazil','Mexico','China','India','Japan','United States','Canada','United Kingdom') then 'CSB9' 	
		when pgh.ctry_desc = 'Australia' then 'APJ CSES' 
		when pgh.ctry_desc in ('France','Germany') then 'EMEA CSES' end as SALES_SEG_LVL1,
	case when pgh.ctry_desc in ('Brazil','Mexico','China','India','Japan','United States','Canada','United Kingdom') then pgh.ctry_desc	
		when pgh.ctry_desc = 'Australia' then 'ANZ' 
		when pgh.ctry_desc in ('France','Germany') then pgh.ctry_desc end as SALES_SEG_LVL2,
	case when pgh.ctry_desc in ('Brazil','Mexico','China','India','Japan','United States','Canada','United Kingdom') then
			case 
				when ch.seg_code = 'COSLA' then 'GDO'
				when ch.seg_code = 'COSLR' then 'Retail'
				else 'Direct' end	
		when pgh.ctry_desc = 'Australia' then pgh.ctry_desc
		when pgh.ctry_desc in ('France','Germany') then 'Consumer' end as SALES_SEG_LVL3,
	case when pgh.ctry_desc in ('Brazil','Mexico','China','India','Japan','United States','Canada','United Kingdom') then
			case 
				when ch.seg_code not in  ('COSLA','COSLR') then 'Consumer'
				else null end	
		when pgh.ctry_desc = 'Australia' then 'Consumer'
		when pgh.ctry_desc in ('France','Germany') then null end as SALES_SEG_LVL4,
	case when pgh.ctry_desc = 'United States' and ch.seg_code not in  ('COSLA','COSLR')  then 
			case 
				when ch.seg_code = 'CSEPP' then 'MPP'
				when ch.seg_code = 'COSLO' then 'Online'
				else 'Offline'
			end
	else null end as SALES_SEG_LVL5,	
	'[' || a.account_id || '] ' || ch.lcl_chnl_desc as SALES_SEG_LVL6,	
	'Consumer' as GLOBAL_SEGMENT,
	wt.weight_id as WEIGHT_ID,
	case when pgh.ctry_desc in ('United States','Canada') then 'NA'	
		when pgh.rgn_desc = 'Americas' then 'LA'
		else pgh.rgn_desc end AS REGION,
	case when pgh.ctry_desc = 'Australia' then 'ANZ' else pgh.ctry_desc end as SUB_REGION,	
	pgh.ctry_desc as COUNTRY,
	null as RAD_CODE,	
	null as GLOBAL_ACCOUNT_ID,	
	null as GLOBAL_ACCOUNT_NAME,	
	null as GMM,	
	null as PARTNER_REL_TYPE,	
	null as PARTNER_TYPE,	
	null as PARTNER_TIER,	
	null as PARTNER_CERT,	
	null as SERVICES_INDUSTRY
from sas_pulse_stg.FY16H2_NPS_AUDIENCE a
inner join corp_drm_pkg.phys_geo_hier pgh on substring(a.account_id,1,position('-' in  a.account_id)-1) = pgh.bu_id    
inner join corp_drm_pkg.chnl_hier  ch on substring(a.account_id,1,position('-' in  a.account_id)-1) 
	= ch.bu_id and substring(a.account_id,position('-' in  a.account_id)+1,character_length(a.account_id)) = ch.lcl_chnl_code
left outer join sas_pulse_stg.COMM_NPS_CTRY_SEG_WT wt on pgh.iso_ctry_code_2 = wt.iso_country_code and wt.global_segment = 'CONS' and case when ch.seg_code = 'COSLR' then 'Retail' when ch.seg_code = 'COSLA' then 'GDO' else 'Direct' end = wt.sub_segment
where survey = 'B2C NPS' and a.ACCOUNT_ID not in (select ACCOUNT_ID from SAS_PULSE_STG.medallia_nps_acct_fy16h2)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24;

insert into SAS_PULSE_STG.medallia_nps_acct_fy16h2_2
select * from tmp_work_db.b2b_acct_fy16h2_medallia_6;