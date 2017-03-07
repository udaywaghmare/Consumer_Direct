SELECT
SO_DT(FORMAT 'MMM')(CHAR(3)) AS MTH,
CUST_BU_ID,
sls_chnl_cd,
pgh.ctry_desc,
'new' AS WAVE,
COUNT(*),
SUM(TOT_REVN_DISC_AMT) AS TOT_REV
FROM tmp_work_db.ev_gbl_cons_nps_tmp1 T
INNER JOIN corp_drm_pkg.phys_geo_hier pgh     ON    T.CUST_BU_ID = pgh.BU_ID
GROUP BY 1,2,3,4,5
UNION
SELECT
SO_DT(FORMAT 'MMM')(CHAR(3)) AS MTH,
CUST_BU_ID,
sls_chnl_cd,
pgh.ctry_desc,
'old' AS WAVE,
COUNT(*),
SUM(TOT_REVN_DISC_AMT) AS TOT_REV
FROM sas_pulse_stg.ev_gbl_cons_nps_tmp1 T
INNER JOIN corp_drm_pkg.phys_geo_hier pgh     ON    T.CUST_BU_ID = pgh.BU_ID
GROUP BY 1,2,3,4,5
;


SELECT
T.CUST_BU_ID,
T.cust_lcl_chnl_cd,
ch.bu_id,
ch.lcl_chnl_code,
ch.type_code,
ch.cust_type_code,
ch.seg_code, 
ch.vert_code,
pgh.ctry_desc,
case when O.CUST_BU_ID is not null then 'Y' else 'N' end as Last_Wave ,
count(*)
FROM SAS_PULSE_STG.ev_gbl_cons_nps_tmp1 T
LEFT JOIN corp_drm_pkg.phys_geo_hier pgh     ON    T.CUST_BU_ID = pgh.BU_ID
LEFT JOIN corp_drm_pkg.chnl_hier ch ON T.CUST_BU_ID = ch.bu_id AND T.cust_lcl_chnl_cd = ch.lcl_chnl_code
LEFT JOIN tmp_work_db.ev_gbl_cons_nps_tmp1 O ON T.CNSLD_SRC_TXN_ID=O.CNSLD_SRC_TXN_ID AND T.CNSLD_SRC_TXN_BU_ID=O.CNSLD_SRC_TXN_BU_ID
--WHERE CTRY_DESC ='United States' AND T.evnt_dt > '2015-06-01'
WHERE T.evnt_dt > '2015-06-01' --O.CUST_BU_ID is null -- and t.tot_revn_disc_amt > 1000
GROUP BY 1,2,3,4,5,6,7,8,9,10