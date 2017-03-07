select Count(1) From sas_pulse_stg.ev_gbl_cons_nps_tmp1a_prev_q; --3142526
select Count(1) From sas_pulse_stg.ev_gbl_cons_nps_tmp1a; ---3140685



DELETE FROM sas_pulse_stg.ev_gbl_cons_nps_tmp1a_prev_q ALL;
Insert Into sas_pulse_stg.ev_gbl_cons_nps_tmp1a_prev_q
Select * From sas_pulse_stg.ev_gbl_cons_nps_tmp1a;


DELETE FROM sas_pulse_stg.ev_gbl_cons_nps_tmp1a ALL;

INSERT INTO sas_pulse_stg.ev_gbl_cons_nps_tmp1a
SELECT tmp.CNSLD_SRC_TXN_ID, tmp.CNSLD_SRC_TXN_BU_ID, MAX(cph.COMB_HIER_CD) AS SYS_COMB_HIER_CD
FROM	sas_pulse_stg.ev_gbl_cons_nps_tmp1 tmp
INNER JOIN (SELECT	ORD_NBR, SRC_BU_ID, sys_flg, extrnl_comb_hier_cd
FROM	sls_pkg.so_dtl_fact 
WHERE	tmzn_loc_id IN(1,2,3,4,5)
	AND ord_dt >  '2015-03-28') odf  		
	ON	tmp.CNSLD_SRC_TXN_ID = odf.ORD_NBR 
	AND	tmp.CNSLD_SRC_TXN_BU_ID = odf.SRC_BU_ID
INNER JOIN itm_pkg.comb_prod_hier cph 		
	ON	odf.extrnl_comb_hier_cd = cph.comb_hier_cd
WHERE  cph.sys_flg = 'Y' OR odf.sys_flg ='Y'
GROUP BY 1,2;

--00:00:35 2752663

UPDATE tmp1	
FROM sas_pulse_stg.ev_gbl_cons_nps_tmp1 tmp1, sas_pulse_stg.ev_gbl_cons_nps_tmp1a  tmp1a
SET sys_flg = 'Y', SYS_COMB_HIER_CD = tmp1a.SYS_COMB_HIER_CD	
WHERE tmp1.CNSLD_SRC_TXN_BU_ID = tmp1a.CNSLD_SRC_TXN_BU_ID AND tmp1.CNSLD_SRC_TXN_ID = tmp1a.CNSLD_SRC_TXN_ID;

--00:00:01	2752663

