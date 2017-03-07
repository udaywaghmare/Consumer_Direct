Select Count(1) From sas_pulse_stg.ev_gbl_cons_nps_tmp1b_prev_q; --3681811
Select Count(1) From sas_pulse_stg.ev_gbl_cons_nps_tmp1b; --3599485

Delete sas_pulse_stg.ev_gbl_cons_nps_tmp1b_prev_q;
Insert into sas_pulse_stg.ev_gbl_cons_nps_tmp1b_prev_q
Select * From sas_pulse_stg.ev_gbl_cons_nps_tmp1b;



DELETE FROM sas_pulse_stg.ev_gbl_cons_nps_tmp1b ALL;

INSERT INTO sas_pulse_stg.ev_gbl_cons_nps_tmp1b
SELECT tmp.CNSLD_SRC_TXN_ID, tmp.CNSLD_SRC_TXN_BU_ID, cph.COMB_HIER_CD AS SNP_COMB_HIER_CD
FROM	sas_pulse_stg.ev_gbl_cons_nps_tmp1 tmp
INNER JOIN (SELECT	ORD_NBR, SRC_BU_ID,  extrnl_comb_hier_cd, snp_flg, SNP_REVN_RTL_AMT QUALIFY RANK() OVER (PARTITION BY src_bu_id, ord_nbr ORDER BY 
	SNP_REVN_RTL_AMT DESC, ITM_NBR ASC, CMPNT_ITM_NBR ASC, ORD_DTL_SEQ_NBR ASC) = 1
FROM	sls_pkg.so_dtl_fact 
WHERE	tmzn_loc_id IN(1,2,3,4,5)
	AND ord_dt >  '2015-03-28') odf  		
	ON	tmp.CNSLD_SRC_TXN_ID = odf.ORD_NBR 
	AND	tmp.CNSLD_SRC_TXN_BU_ID = odf.SRC_BU_ID
INNER JOIN itm_pkg.comb_prod_hier cph 		
	ON	odf.extrnl_comb_hier_cd = cph.comb_hier_cd AND cph.PROD_OFFRG_DESC NOT IN 
		('CONSUMABLES - INKJET','PRINTERS - INKJET')
WHERE  (odf.snp_flg = 'Y')	OR (odf.SNP_REVN_RTL_AMT > 0) OR (cph.prod_grp_cd IN 
	('GRP2SNP', 'GRP33RDPRTY', '3HW'))
GROUP BY 1,2,3;

--00:02:03	3288125

UPDATE tmp1	
FROM sas_pulse_stg.ev_gbl_cons_nps_tmp1 tmp1, sas_pulse_stg.ev_gbl_cons_nps_tmp1b  tmp1b
SET snp_flg = 'Y', SNP_COMB_HIER_CD = tmp1b.SNP_COMB_HIER_CD	
WHERE tmp1.CNSLD_SRC_TXN_BU_ID = tmp1b.CNSLD_SRC_TXN_BU_ID AND tmp1.CNSLD_SRC_TXN_ID = tmp1b.CNSLD_SRC_TXN_ID;

--00:00:01	3288125
