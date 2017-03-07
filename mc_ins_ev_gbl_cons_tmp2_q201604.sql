Select Count(1) From sas_pulse_stg.ev_gbl_cons_nps_tmp2_prev_q; --4219522
Select Count(1) From sas_pulse_stg.ev_gbl_cons_nps_tmp2; --3958042

Delete sas_pulse_stg.ev_gbl_cons_nps_tmp2_prev_q all;
Insert into sas_pulse_stg.ev_gbl_cons_nps_tmp2_prev_q
Select * From sas_pulse_stg.ev_gbl_cons_nps_tmp2;


delete from sas_pulse_stg.ev_gbl_cons_nps_tmp2 ALL;

INSERT INTO sas_pulse_stg.ev_gbl_cons_nps_tmp2
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
	sys_comb_hier_cd,
	snp_comb_hier_cd
)
SELECT
	tmp1.evnt_ldr_id,
	tmp1.evnt_type_cd,
	tmp1.evnt_subtype_cd,
	tmp1.evnt_dt,
	tmp1.evnt_dts,
	tmp1.cnsld_src_txn_id,
	tmp1.cnsld_src_txn_bu_id,
	tmp1.so_nbr,
	tmp1.so_bu_id,
	tmp1.so_lcl_chnl_cd,
	tmp1.so_dt,
	tmp1.so_inv_dt,
	tmp1.qte_nbr,
	tmp1.qte_bu_id,
	tmp1.qte_lcl_chnl_cd,
	tmp1.qte_dt,
	tmp1.prim_sls_assoc_nbr,
	tmp1.secnd_sls_assoc_nbr,
	tmp1.sys_itm_cls_nm,
	tmp1.sys_flg,
	tmp1.snp_flg,
	tmp1.svcs_flg,
	tmp1.sls_chnl_cd,
	tmp1.tot_revn_disc_amt,
	tmp1.ord_dispos_stat_cd,
	tmp1.sls_type_cd,
	tmp1.cust_bu_id,
	tmp1.cust_lcl_chnl_cd,
	tmp1.bilt_cust_nbr,
	tmp1.bilt_src_cust_id,
	tmp1.bilt_addr_seq_id,
	tmp1.bilt_cntct_id,
	tmp1.shpto_cust_nbr,
	tmp1.shpto_src_cust_id,
	tmp1.shpto_addr_seq_id,
	tmp1.shpto_cntct_id,
	tmp1.sys_comb_hier_cd,
	tmp1.snp_comb_hier_cd
FROM sas_pulse_stg.ev_gbl_cons_nps_tmp1 tmp1
INNER JOIN
	(SELECT MAX(so_nbr) AS so_nbr, cust_bu_id, bilt_cust_nbr
	  FROM sas_pulse_stg.ev_gbl_cons_nps_tmp1
	  GROUP BY cust_bu_id, bilt_cust_nbr) mso
	ON tmp1.so_nbr = mso.so_nbr AND tmp1.cust_bu_id = mso.cust_bu_id
	AND tmp1.bilt_cust_nbr = mso.bilt_cust_nbr
QUALIFY RANK() OVER (PARTITION BY tmp1.cust_bu_id, tmp1.bilt_cust_nbr, tmp1.shpto_cust_nbr ORDER BY tmp1.so_nbr DESC) = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38
UNION 
SELECT
	tmp1.evnt_ldr_id,
	tmp1.evnt_type_cd,
	tmp1.evnt_subtype_cd,
	tmp1.evnt_dt,
	tmp1.evnt_dts,
	tmp1.cnsld_src_txn_id,
	tmp1.cnsld_src_txn_bu_id,
	tmp1.so_nbr,
	tmp1.so_bu_id,
	tmp1.so_lcl_chnl_cd,
	tmp1.so_dt,
	tmp1.so_inv_dt,
	tmp1.qte_nbr,
	tmp1.qte_bu_id,
	tmp1.qte_lcl_chnl_cd,
	tmp1.qte_dt,
	tmp1.prim_sls_assoc_nbr,
	tmp1.secnd_sls_assoc_nbr,
	tmp1.sys_itm_cls_nm,
	tmp1.sys_flg,
	tmp1.snp_flg,
	tmp1.svcs_flg,
	tmp1.sls_chnl_cd,
	tmp1.tot_revn_disc_amt,
	tmp1.ord_dispos_stat_cd,
	tmp1.sls_type_cd,
	tmp1.cust_bu_id,
	tmp1.cust_lcl_chnl_cd,
	tmp1.bilt_cust_nbr,
	tmp1.bilt_src_cust_id,
	tmp1.bilt_addr_seq_id,
	tmp1.bilt_cntct_id,
	tmp1.shpto_cust_nbr,
	tmp1.shpto_src_cust_id,
	tmp1.shpto_addr_seq_id,
	tmp1.shpto_cntct_id,
	tmp1.sys_comb_hier_cd,
	tmp1.snp_comb_hier_cd
FROM sas_pulse_stg.ev_gbl_cons_nps_tmp1 tmp1
WHERE cnsld_src_txn_bu_id = 8270 AND cnsld_src_txn_id IN ( SELECT so_nbr FROM sas_pulse_Stg.SB_DDW_CN_USERS )

--00:00:13	3409926

