Select Count(1) From sas_pulse_stg.ev_gbl_cons_nps_tmp1c_prev_q; --7180469
Select Count(1) From sas_pulse_stg.ev_gbl_cons_nps_tmp1c; --6897444

Delete sas_pulse_stg.ev_gbl_cons_nps_tmp1c_prev_q All;
Insert into sas_pulse_stg.ev_gbl_cons_nps_tmp1c_prev_q
Select * From sas_pulse_stg.ev_gbl_cons_nps_tmp1c;
 

DELETE FROM sas_pulse_stg.ev_gbl_cons_nps_tmp1c ALL;

INSERT INTO sas_pulse_stg.ev_gbl_cons_nps_tmp1c
SELECT tmp.CNSLD_SRC_TXN_ID, tmp.CNSLD_SRC_TXN_BU_ID, MAX(odf.sls_type_cd) AS SLS_TYPE_CD
FROM	sas_pulse_stg.ev_gbl_cons_nps_tmp1 tmp
INNER JOIN (SELECT	ORD_NBR, SRC_BU_ID,  extrnl_comb_hier_cd, sls_type_cd
FROM	sls_pkg.so_dtl_fact 
WHERE	tmzn_loc_id IN(1,2,3,4,5)
	AND ord_dt >    '2014-03-28') odf  		
	ON	tmp.CNSLD_SRC_TXN_ID = odf.ORD_NBR 
	AND	tmp.CNSLD_SRC_TXN_BU_ID = odf.SRC_BU_ID	
GROUP BY 1,2;

--00:00:28	6161313

UPDATE tmp1	
FROM sas_pulse_stg.ev_gbl_cons_nps_tmp1 tmp1, sas_pulse_stg.ev_gbl_cons_nps_tmp1c  tmp1c
SET SLS_TYPE_CD = tmp1c.SLS_TYPE_CD
WHERE tmp1.CNSLD_SRC_TXN_BU_ID = tmp1c.CNSLD_SRC_TXN_BU_ID AND tmp1.CNSLD_SRC_TXN_ID = tmp1c.CNSLD_SRC_TXN_ID;

--00:00:03	6161313
