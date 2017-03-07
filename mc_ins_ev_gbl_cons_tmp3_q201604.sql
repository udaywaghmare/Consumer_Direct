Select Count(1) From sas_pulse_stg.ev_gbl_cons_nps_tmp3_prev_q; --4219522
Select Count(1) From sas_pulse_stg.ev_gbl_cons_nps_tmp3; ---3958042

Delete  sas_pulse_stg.ev_gbl_cons_nps_tmp3_prev_q All;
Insert into  sas_pulse_stg.ev_gbl_cons_nps_tmp3_prev_q
Select * From  sas_pulse_stg.ev_gbl_cons_nps_tmp3;

delete from sas_pulse_stg.ev_gbl_cons_nps_tmp3 ALL;

INSERT INTO sas_pulse_stg.ev_gbl_cons_nps_tmp3
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
	shpto_cntct_id,
	SYS_COMB_HIER_CD,
	SNP_COMB_HIER_CD
)
SELECT
	tmp2.evnt_ldr_id,
	tmp2.evnt_type_cd,
	tmp2.evnt_subtype_cd,
	tmp2.evnt_dt,
	tmp2.evnt_dts,
	tmp2.cnsld_src_txn_id,
	tmp2.cnsld_src_txn_bu_id,
	tmp2.so_nbr,
	tmp2.so_bu_id,
	tmp2.so_lcl_chnl_cd,
	tmp2.so_dt,
	tmp2.so_inv_dt,
	tmp2.qte_nbr,
	tmp2.qte_bu_id,
	tmp2.qte_lcl_chnl_cd,
	tmp2.qte_dt,
	tmp2.prim_sls_assoc_nbr,
	tmp2.secnd_sls_assoc_nbr,
	tmp2.sys_itm_cls_nm,
	tmp2.sys_flg,
	tmp2.snp_flg,
	tmp2.svcs_flg,
	tmp2.sls_chnl_cd,
	tmp2.tot_revn_disc_amt,
	tmp2.ord_dispos_stat_cd,
	tmp2.sls_type_cd,
	tmp2.cust_bu_id,
	tmp2.cust_lcl_chnl_cd,
	tmp2.bilt_cust_nbr,
	tmp2.bilt_src_cust_id,
	tmp2.bilt_addr_seq_id,
	tmp2.bilt_cntct_id,
	tmp2.shpto_cust_nbr,
	tmp2.shpto_src_cust_id,
	tmp2.shpto_addr_seq_id,
	tmp2.shpto_cntct_id,
	tmp2.SYS_COMB_HIER_CD,
	tmp2.SNP_COMB_HIER_CD
FROM sas_pulse_stg.ev_gbl_cons_nps_tmp2 tmp2
WHERE NOT EXISTS 
	(SELECT 'X' 
	 FROM ce_base.survey_evnt e
	 WHERE tmp2.evnt_ldr_id = e.evnt_ldr_id
		AND tmp2.evnt_type_cd = e.evnt_type_cd
		AND tmp2.cnsld_src_txn_id = e.cnsld_src_txn_id
		AND tmp2.cnsld_src_txn_bu_id = e.cnsld_src_txn_bu_id
		AND tmp2.evnt_subtype_cd = e.evnt_subtype_cd);

--00:00:06	3409926

