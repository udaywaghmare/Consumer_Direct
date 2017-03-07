--CREATE SET TABLE SAS_PULSE_STG.CSB_NPS_AUDIENCE_CON_DDW AS SAS_PULSE_STG.fy17H2m10_nps_audience WITH NO DATA; 
delete from sas_pulse_stg.ev_gbl_cons_nps_inc_tmp all;
insert into sas_pulse_stg.ev_gbl_cons_nps_inc_tmp
SELECT 		
	tmp.*
FROM sas_pulse_stg.ev_gbl_cons_nps_inc tmp 		
INNER JOIN corp_drm_pkg.PHYS_GEO_HIER pgh ON tmp.cust_bu_id = pgh.bu_id 		
INNER JOIN corp_drm_pkg.chnl_hier ch ON tmp.cust_bu_id = ch.BU_ID AND tmp.cust_lcl_chnl_cd = ch.LCL_CHNL_CODE 		
INNER join itm_pkg.comb_prod_hier prod on coalesce(tmp.attr_2_val, tmp.attr_3_val) = prod.comb_hier_cd		
WHERE 
tmp.email_addr_id NOT IN (SELECT TRIM(es.email_address) FROM marcom.email_subscriber es WHERE es.emailable_flag = 'N')
AND tmp.email_addr_id NOT IN 
(
sel email_address from  SAS_PULSE_STG.fy17h2m11_nps_audience union 
sel email_address from  SAS_PULSE_STG.FY17H2M12_NPS_AUDIENCE union 
sel email_addr_id from sas_pulse_stg.comm_nps_aud_inv union
sel email_address from  SAS_PULSE_STG.FY18H1M01_NPS_AUDIENCE  union
sel email_address from  SAS_PULSE_STG.FY18H1M02_NPS_AUDIENCE  
)
AND tmp.email_addr_id NOT IN (SELECT EMAIL_ADDR_ID FROM CE_BASE.SURVEY_DO_NOT_DSTRB_LIST) 	
AND tmp.domain_email_addr_id NOT IN (SELECT EMAIL_ADDR_ID FROM CE_BASE.SURVEY_DO_NOT_DSTRB_LIST) 	
AND tmp.email_addr_id NOT IN 
(
SEL email_address FROM SAS_PULSE_STG.CSB_NPS_AUDIENCE_SB_DDW UNION 
SEL email_address FROM SAS_PULSE_STG.CSB_NPS_AUDIENCE_SB_SFDC UNION 
SEL email_addr_id AS email_address FROM SAS_PULSE_STG.b2c_ev_gbl_extrnl union
SEL email_address FROM SAS_PULSE_STG.CSB_NPS_AUDIENCE_CON_DDW union
SEL email_address FROM SAS_PULSE_STG.CSB_NPS_AUDIENCE_CON_RTL 
)
AND prod.prod_grp_desc in ('Consumer','CS SOFTWARE AND PERIPHERALS','Tablet Devices')	
AND NOT (ch.bu_id = 3535 and ch.lcl_chnl_code = '35034')
;
delete from SAS_PULSE_STG.b2c_ev_gbl_extrnl all; 
insert into SAS_PULSE_STG.b2c_ev_gbl_extrnl
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
	'FY18 H1' as attr_5_val
from sas_pulse_stg.ev_gbl_cons_nps_inc_tmp
sample
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'US' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 3469
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'US' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 1220
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'US' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 1766
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'US' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 312
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'US' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 3136
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'US' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 1675
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'US' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 1556
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'US' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 359
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'CA' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 460
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'CA' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 220
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'CA' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 117
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'CA' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 20
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'CA' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 330
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'CA' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 150
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'CA' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 168
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'CA' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 33
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'BR' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 873
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'BR' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 5
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'BR' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 38
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'BR' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 1
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'BR' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 733
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'BR' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 4
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'BR' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 36
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'BR' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 1
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'GB' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 525
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'GB' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 12
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'GB' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 180
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'GB' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 5
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'GB' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 550
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'GB' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 36
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'GB' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 37
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'GB' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'JP' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 668
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'JP' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 548
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'JP' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 48
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'JP' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 14
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'JP' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 566
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'JP' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 393
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'JP' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 35
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'JP' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 13
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'CN' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 341
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'CN' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 108
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'CN' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 0
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'CN' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 9
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'CN' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 402
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'CN' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 113
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'CN' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 0
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'CN' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 11


when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'DE' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 428
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'DE' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'DE' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 236
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'DE' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 2
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'DE' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 599
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'DE' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'DE' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 37
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'DE' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'FR' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 503
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'FR' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'FR' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 163
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'FR' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'FR' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 467
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'FR' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'FR' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 90
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'FR' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'AU' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 481
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'AU' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 1
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'AU' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 50
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'AU' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 1
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'AU' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 432
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'AU' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 15
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'AU' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 50
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'AU' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 1

when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'SE' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 78
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'SE' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 1
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'SE' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 15
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'SE' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'SE' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 59
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'SE' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 1
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'SE' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 12
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'SE' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0


/*
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'MX' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 7
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'MX' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 4
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'MX' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 32
when attr_1_val = '0-6 Months' and iso_ctry_cd_2 = 'MX' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 1
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'MX' and sys_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 5
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'MX' and sys_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 0
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'MX' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'ONLINE' then 12
when attr_1_val = '7-12 Months' and iso_ctry_cd_2 = 'MX' and sys_flg is null and snp_flg = 'Y' and sls_chnl_cd = 'OFFLINE' then 1
*/

end	


select max(adnc_seltn_id) from
(
select max(adnc_seltn_id) as adnc_seltn_id from  SAS_PULSE_STG.b2b_adnc_seltn_ids union
select max(adnc_seltn_id) as adnc_seltn_id from  SAS_PULSE_STG.b2c_adnc_seltn_ids
) xc

delete from SAS_PULSE_STG.b2c_adnc_seltn_ids all;
insert into SAS_PULSE_STG.b2c_adnc_seltn_ids
select
	(ROW_NUMBER() OVER (ORDER BY cnsld_src_txn_id, cnsld_src_txn_bu_id) + 811870085),
	 cnsld_src_txn_id, cnsld_src_txn_bu_id
from SAS_PULSE_STG.b2c_ev_gbl_extrnl
;
DELETE FROM SAS_PULSE_STG.CSB_NPS_AUDIENCE_CON_DDW ALL;
INSERT INTO SAS_PULSE_STG.CSB_NPS_AUDIENCE_CON_DDW
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
'FY18 H1'             AS          WAVE   ,
'No'       AS          OVERSAMPLE_FLAG       ,
ev.attr_1_val   AS          PURCHASE_RECENCY    ,
TRIM(TRAILING '.' FROM CAST(ev.SO_BU_ID AS  VARCHAR(20))) || '-' ||TRIM(ev.SO_NBR)            AS          ORDER_NUMBER             ,
TRIM(TRAILING '.' FROM CAST(ev.SVC_TAG_BU_ID AS  VARCHAR(20))) || '-' ||TRIM(ev.SVC_TAG_ID)       AS          SVC_TAG             ,
CAST(CAST(TRIM(SUBSTR(coalesce(ev.SO_DT,ev.evnt_dt),1,10))               AS  DATE FORMAT'YYYY-MM-DD')         AS VARCHAR(10)) AS  ORDER_DATE     ,
TRIM(TRAILING '.' FROM CAST(ev.CUST_BU_ID AS VARCHAR(20))) || '-' ||TRIM(ev.CUST_NBR)       AS          CUSTOMER_NUMBER   ,

COALESCE( RSP_ST.rsp_status, 'Not-Invited') AS REPEAT_RESPONDERS,
NULL AS ACCOUNT_TYPE,

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
FROM SAS_PULSE_STG.b2c_ev_gbl_extrnl ev
INNER JOIN SAS_PULSE_STG.b2c_adnc_seltn_ids a on ev.cnsld_src_txn_id = a.cnsld_src_txn_id and ev.cnsld_src_txn_bu_id = a.cnsld_src_txn_bu_id
LEFT OUTER JOIN sas_pulse_stg.comm_nps_link_lkup lnk_lkup ON ev.iso_ctry_2_cd = lnk_lkup.iso_ctry_cd AND ev.iso_lang_cd=lnk_lkup.ISO_LANG_CD
inner join itm_pkg.comb_prod_hier prod on coalesce(ev.attr_2_val, ev.attr_3_val)  = prod.comb_hier_cd	
LEFT OUTER JOIN SAS_PULSE_STG.COMM_NPS_PREV_RESP_STATUS rsp_st ON EV.email_addr_ID = rsp_st.email_address
;

update SAS_PULSE_STG.CSB_NPS_AUDIENCE_CON_DDW
set contact_name = trim(td_sysfnlib.oreplace(td_sysfnlib.oreplace(contact_name,'09'XC,' '),'0D0A'XC,' '));  

delete from  SAS_PULSE_STG.CSB_NPS_AUDIENCE_CON_DDW where product_lvl5 in ('DSE2716HN',
'DUZ2315HN',
'DSE2216HN',
'DP2314TN',
'DE2416HN',
'DSE2416HN'
);

create table tmp_work_db.b2b_acct_medallia_6 as SAS_PULSE_STG.medallia_nps_acct_master with no data; 



	'CSB12' AS SALES_SEG_LVL1, 
	pgh.ctry_desc  AS SALES_SEG_LVL2,
	'Retail'  AS SALES_SEG_LVL3,
	'Consumer' AS SALES_SEG_LVL4,
	case when pgh.ctry_desc = 'United States' and ch.seg_code not in  ('COSLA','COSLR')  then 
			case 
				when ch.seg_code = 'CSEPP' then 'MPP'
				when ch.seg_code = 'COSLO' then 'Online'
				else 'Offline'
			end
	else null end as SALES_SEG_LVL5,	
	'[' || a.account_id || '] ' || ch.lcl_chnl_desc as SALES_SEG_LVL6,	
	'Consumer' as GLOBAL_SEGMENT,
	wt.weightid as WEIGHT_ID,
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
	null as SERVICES_INDUSTRY,
	'YES' AS ACTIVE_FLG

insert into tmp_work_db.b2b_acct_medallia_6
select
a.ACCOUNT_ID as ACCOUNT_ID,	
ch.lcl_chnl_desc as ACCOUNT_NAME,	
'Transactional' as CUSTOMER_REL_TYPE,
case when pgh.ctry_desc in ('Brazil','Australia','China','India','Japan','United States','Canada','United Kingdom','France','Germany','Sweden') then 'CSB12' 
	when pgh.ctry_desc = 'Mexico' then 'LA CSES' 
	--when pgh.ctry_desc in () then 'EMEA CSES' 
	end as SALES_SEG_LVL1,
	case when pgh.ctry_desc in ('Brazil','Australia','China','India','Japan','Canada','United Kingdom','France','Germany','Sweden') then pgh.ctry_desc	
		--when pgh.ctry_desc = 'Australia' then 'ANZ' 
		--when pgh.ctry_desc in ('France','Germany') then pgh.ctry_desc end as SALES_SEG_LVL2,
		 when pgh.ctry_desc in ('United States') then 'US'
	end as SALES_SEG_LVL2,
	case when pgh.ctry_desc in ('Brazil','Australia','China','India','Japan','United States','Canada','United Kingdom','France','Germany','Sweden') then
			case 
				when ch.seg_code = 'COSLA' then 'GDO'
				when ch.seg_code = 'COSLR' then 'Retail'
				else 'Direct' end	
		--when pgh.ctry_desc = 'Australia' then pgh.ctry_desc
		--when pgh.ctry_desc in ('France','Germany') then 'Consumer' end as SALES_SEG_LVL3,
	end as SALES_SEG_LVL3,
	case when pgh.ctry_desc in ('Brazil','Australia','China','India','Japan','United States','Canada','United Kingdom','France','Germany','Sweden') then 'Consumer' as SALES_SEG_LVL4,
	case when pgh.ctry_desc = 'United States' and ch.seg_code not in  ('COSLA','COSLR')  then 
			case 
				when ch.seg_code = 'CSEPP' then 'MPP'
				when ch.seg_code = 'COSLO' then 'Online'
				else 'Offline'
			end
	else null end as SALES_SEG_LVL5,	
	'[' || a.account_id || '] ' || ch.lcl_chnl_desc as SALES_SEG_LVL6,	
	'Consumer' as GLOBAL_SEGMENT,
	wt.weightid as WEIGHT_ID,
	case when pgh.ctry_desc in ('United States','Canada') then 'NA'	
		when pgh.rgn_desc = 'Americas' then 'LA'
		else pgh.rgn_desc end AS REGION,
	case when pgh.ctry_desc = 'Australia' then 'ANZ' when pgh.ctry_desc in ('United States') then 'US' else pgh.ctry_desc end as SUB_REGION,	
	case when when pgh.ctry_desc in ('United States') then 'US' else pgh.ctry_desc end as COUNTRY,
	null as RAD_CODE,	
	null as GLOBAL_ACCOUNT_ID,	
	null as GLOBAL_ACCOUNT_NAME,	
	null as GMM,	
	null as PARTNER_REL_TYPE,	
	null as PARTNER_TYPE,	
	null as PARTNER_TIER,	
	null as PARTNER_CERT,	
	null as SERVICES_INDUSTRY,
	'YES' AS ACTIVE_FLG
from SAS_PULSE_STG.CSB_NPS_AUDIENCE_CON_DDW a
inner join corp_drm_pkg.phys_geo_hier pgh on substring(a.account_id,1,position('-' in  a.account_id)-1) = pgh.bu_id    
inner join corp_drm_pkg.chnl_hier  ch on substring(a.account_id,1,position('-' in  a.account_id)-1) 
	= ch.bu_id and substring(a.account_id,position('-' in  a.account_id)+1,character_length(a.account_id)) = ch.lcl_chnl_code
--left outer join sas_pulse_stg.COMM_NPS_CTRY_SEG_WT wt on pgh.iso_ctry_code_2 = wt.iso_country_code and wt.global_segment = 'CONS' and case when ch.seg_code = 'COSLR' then 'Retail' when ch.seg_code = 'COSLA' then 'GDO' else 'Direct' end = wt.sub_segment
left outer join SAS_GBL_CEANALYTICS_STAG.rsc_FY17_rev_SSL4_BU wt on  '[' || a.account_id || '] ' || ch.lcl_chnl_desc =WT.SSL6
where survey = 'B2C NPS' and a.ACCOUNT_ID not in (select ACCOUNT_ID from SAS_PULSE_STG.medallia_nps_acct_master)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
;
