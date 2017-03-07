/*
CREATE MULTISET TABLE tmp_work_db.ev_gbl_cons_nps_tmp4_x ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      EVNT_SUBTYPE_CD CHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      CNSLD_SRC_TXN_ID VARCHAR(40) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      CNSLD_SRC_TXN_BU_ID INTEGER NOT NULL,
      ISO_CTRY_CD_2 CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC COMPRESS ('CA','CN','DE','FR','GB','JP','MX','US'),
      ISO_LANG_CD CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      CUST_CO_NM VARCHAR(250) CHARACTER SET UNICODE NOT CASESPECIFIC,
      FRST_NM VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      LAST_NM VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC,
      FULL_NM VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
      EMAIL_ADDR_ID VARCHAR(254) CHARACTER SET UNICODE NOT CASESPECIFIC)
PRIMARY INDEX UPI_EV_GBL_CONS_NPS_TMP4 ( EVNT_SUBTYPE_CD ,CNSLD_SRC_TXN_ID ,CNSLD_SRC_TXN_BU_ID );

insert into sas_pulse_stg.ev_gbl_cons_nps_tmp4
(
	evnt_subtype_cd,
	cnsld_src_txn_id,
	cnsld_src_txn_bu_id,
	iso_ctry_cd_2,
	iso_lang_cd,
	cust_co_nm,
	frst_nm,
	last_nm,
	full_nm,
	email_addr_id
)
SEL
evnt_subtype_cd,
CNSLD_SRC_TXN_ID			,
CNSLD_SRC_TXN_BU_ID			,
MAX(	ISO_CTRY_CD_2	)	,
MAX(	ISO_LANG_CD	)	,
MAX(	CUST_CO_NM	)	,
MAX(	FRST_NM	)	,
MAX(	LAST_NM	)	,
MAX(	FULL_NM	)	,
MAX(	EMAIL_ADDR_ID	)		
FROM tmp_work_db.ev_gbl_cons_nps_tmp4_x
GROUP BY 1,2,3
*/



DELETE FROM sas_pulse_stg.ev_gbl_cons_nps_cce ALL;
INSERT INTO sas_pulse_stg.ev_gbl_cons_nps_cce 
SEL cce.first_name,
cce.last_name,
cce.address_seq_num,
cce.customer_contact_seq_num,
cce.business_unit_id
FROM euro_fin_mart.customer_contact_euro cce
WHERE cce.address_seq_num IN (SEL bilt_addr_seq_id FROM sas_pulse_stg.ev_gbl_cons_nps_tmp3)
AND cce.customer_contact_seq_num IN (SEL bilt_cntct_id FROM sas_pulse_stg.ev_gbl_cons_nps_tmp3)
;
COLLECT STATISTICS COLUMN (FIRST_NAME ,LAST_NAME) ON  sas_pulse_stg.ev_gbl_cons_nps_cce;
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN CUST_BU_ID; 
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN BILT_SRC_CUST_ID;  
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN SHPTO_SRC_CUST_ID; 
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN (CNSLD_SRC_TXN_BU_ID ,BILT_SRC_CUST_ID);
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN (CNSLD_SRC_TXN_BU_ID ,SHPTO_SRC_CUST_ID);
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN (CNSLD_SRC_TXN_BU_ID ,BILT_SRC_CUST_ID ,BILT_ADDR_SEQ_ID ,BILT_CNTCT_ID); 
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN (CNSLD_SRC_TXN_BU_ID ,SHPTO_SRC_CUST_ID ,SHPTO_ADDR_SEQ_ID ,SHPTO_CNTCT_ID); 
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN BILT_CUST_NBR; 
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN SHPTO_CUST_NBR; 
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN (SO_BU_ID ,BILT_CUST_NBR);
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN (SO_BU_ID ,BILT_CUST_NBR ,BILT_ADDR_SEQ_ID);
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN (SO_BU_ID ,SHPTO_CUST_NBR);
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN (SO_BU_ID ,BILT_CUST_NBR ,SHPTO_CUST_NBR ,SHPTO_ADDR_SEQ_ID);
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN (CUST_BU_ID ,BILT_ADDR_SEQ_ID ,BILT_CNTCT_ID);
COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN (CUST_BU_ID ,BILT_CUST_NBR);

Select count(1) From sas_pulse_stg.ev_gbl_cons_nps_tmp4_prev_q; --4219522
Select count(1) From sas_pulse_stg.ev_gbl_cons_nps_tmp4; --3958042

delete from sas_pulse_stg.ev_gbl_cons_nps_tmp4_prev_q ALL;
Insert Into sas_pulse_stg.ev_gbl_cons_nps_tmp4_prev_q 
Select * From sas_pulse_stg.ev_gbl_cons_nps_tmp4;


delete from sas_pulse_stg.ev_gbl_cons_nps_tmp4 ALL;

insert into sas_pulse_stg.ev_gbl_cons_nps_tmp4
(
	evnt_subtype_cd,
	cnsld_src_txn_id,
	cnsld_src_txn_bu_id,
	iso_ctry_cd_2,
	iso_lang_cd,
	cust_co_nm,
	frst_nm,
	last_nm,
	full_nm,
	email_addr_id
)

SELECT
	tmp3.evnt_subtype_cd,
	tmp3.cnsld_src_txn_id,
	tmp3.cnsld_src_txn_bu_id,
	CAST(COALESCE(pgh.iso_ctry_code_2, (CASE 
		WHEN bc.elec_addr IS NOT NULL THEN bc.ctry_cd
		WHEN sc.elec_addr IS NOT NULL THEN sc.ctry_cd
		ELSE NULL END)) AS VARCHAR(2)) AS iso_ctry_2_cd,
	CAST(NULL AS VARCHAR(2)) AS iso_lang_cd,	
	CAST(TRIM(CASE 
		WHEN bc.elec_addr IS NOT NULL THEN COALESCE(bcu.cust_nm, bc.cust_nm)
		WHEN sc.elec_addr IS NOT NULL THEN COALESCE(scu.cust_nm, sc.cust_nm)
		ELSE NULL END) AS VARCHAR(250)) AS cust_co_nm,	
	CAST(TRIM(CASE 
		WHEN bc.elec_addr IS NOT NULL THEN bc.frst_nm
		WHEN sc.elec_addr IS NOT NULL THEN sc.frst_nm
		ELSE NULL END) AS VARCHAR(100)) AS x_frst_nm,
	CAST(TRIM(CASE 
		WHEN bc.elec_addr IS NOT NULL THEN bc.last_nm
		WHEN sc.elec_addr IS NOT NULL THEN sc.last_nm
		ELSE NULL END) AS VARCHAR(100)) AS x_last_nm,
	CAST(TRIM(CASE
		WHEN x_frst_nm IS NOT NULL OR x_last_nm IS NOT NULL THEN COALESCE(x_frst_nm,'') || ' ' || COALESCE(x_last_nm,'')
		ELSE NULL END) AS VARCHAR(200)) AS full_nm,
	CAST(TRIM(COALESCE(bc.elec_addr, sc.elec_addr)) AS VARCHAR(254)) AS email_addr_id
FROM sas_pulse_stg.ev_gbl_cons_nps_tmp3 tmp3
LEFT OUTER JOIN party_pkg.sls_svc_cust bcu ON tmp3.bilt_src_cust_id = bcu.src_cust_id	AND tmp3.cnsld_src_txn_bu_id = bcu.src_bu_id	
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
			elec_addr
			QUALIFY RANK() OVER (PARTITION BY  src_cust_id ORDER BY elec_addr DESC, DW_INS_UPD_DTS DESC, COALESCE(PRIM_EMAIL_FLG, 'N') DESC,  cust_nm DESC, last_nm DESC) = 1
		 FROM party_pkg.sls_svc_cust_ph_cntct_bridge
		 WHERE addr_cntct_role_cd = 'BILL_TO' AND elec_addr IS NOT NULL 	
		 GROUP BY
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
			DW_INS_UPD_DTS,
			PRIM_EMAIL_FLG )  bc 	
ON tmp3.cnsld_src_txn_bu_id = bc.src_bu_id AND tmp3.bilt_src_cust_id = bc.src_cust_id AND tmp3.bilt_addr_seq_id = bc.src_postal_addr_id  AND tmp3.bilt_cntct_id = bc.src_cntct_id		
LEFT OUTER JOIN party_pkg.sls_svc_cust scu ON tmp3.shpto_src_cust_id = scu.src_cust_id	AND tmp3.cnsld_src_txn_bu_id = scu.src_bu_id	
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
			elec_addr
			QUALIFY RANK() OVER (PARTITION BY src_cust_id ORDER BY elec_addr DESC, DW_INS_UPD_DTS DESC, COALESCE(PRIM_EMAIL_FLG, 'N')  DESC, cust_nm DESC, last_nm DESC ) = 1
		 FROM party_pkg.sls_svc_cust_ph_cntct_bridge
		 WHERE addr_cntct_role_cd = 'SHIP_TO' AND elec_addr IS NOT NULL 	
		 GROUP BY
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
			DW_INS_UPD_DTS,
			PRIM_EMAIL_FLG)  sc 	
ON tmp3.cnsld_src_txn_bu_id = sc.src_bu_id AND tmp3.shpto_src_cust_id = sc.src_cust_id AND tmp3.shpto_addr_seq_id = sc.src_postal_addr_id  AND tmp3.shpto_cntct_id = sc.src_cntct_id		
INNER JOIN corp_drm_pkg.phys_geo_hier pgh ON tmp3.cust_bu_id = pgh.bu_id
GROUP BY 1,2,3,4,5,6,7,8,9,10


union

--AMER
select
	tmp3.evnt_subtype_cd,
	tmp3.cnsld_src_txn_id,
	tmp3.cnsld_src_txn_bu_id,
	cast(pgh.iso_ctry_code_2 as char(2)) as iso_ctry_cd_2,
	cast(case when tmp3.so_bu_id = 707 and coalesce(pa.state_prov_code,pas.state_prov_code) = 'QC' then 'FR' else null end as char(2)) as iso_lang_cd,
	cast(trim(coalesce((case when pa.company_name is null then full_nm else pa.company_name end), 
		(case when pas.company_name is null then full_nm else pas.company_name end))) as varchar(250)) as cust_co_nm,
	cast(trim(coalesce(c.first_name, sc.first_name)) as varchar(100)) as frst_nm,
	cast(trim(coalesce(c.last_name, sc.last_name)) as varchar(100)) as last_nm,
	cast(trim(coalesce((case when (c.first_name is not null or c.last_name is not null) 
			then (trim(coalesce(c.first_name, '')) || ' ' || trim(coalesce(c.last_name, ''))) else null end),
			(case when (sc.first_name is not null or sc.last_name is not null) 
			then (trim(coalesce(sc.first_name, '')) || ' ' || trim(coalesce(sc.last_name, ''))) else null end))) as varchar(200)) as full_nm,
	cast(trim(coalesce(ctm.media_value,ctms.media_value)) as varchar(254)) as email_addr_id
from sas_pulse_stg.ev_gbl_cons_nps_tmp3 tmp3
left outer join finance.contact c 
	on tmp3.bilt_cust_nbr = c.customer_num 
	and tmp3.so_bu_id = c.business_unit_id
	and tmp3.bilt_addr_seq_id = c.address_seq_num
	and c.address_type_code = 'B'
left outer join finance.contact_media ctm
	on c.customer_num = ctm.customer_num
	and c.business_unit_id = ctm.business_unit_id
	and c.contact_id = ctm.contact_id
	and ctm.media_type_code in ('EML','E')
left outer join finance.postal_address pa
	on tmp3.bilt_cust_nbr = pa.customer_num 
	and tmp3.bilt_addr_seq_id = pa.address_seq_num 
	and tmp3.so_bu_id = pa.business_unit_id 
	and pa.address_type_code = 'B'
left outer join finance.contact sc 
	on tmp3.bilt_cust_nbr = sc.customer_num 
	and tmp3.shpto_cust_nbr = sc.customer_num
	and tmp3.so_bu_id = sc.business_unit_id
	and tmp3.shpto_addr_seq_id = sc.address_seq_num
	and sc.address_type_code = 'S'
left outer join finance.contact_media ctms
	on sc.customer_num = ctms.customer_num
	and sc.business_unit_id = ctms.business_unit_id
	and sc.contact_id = ctms.contact_id
	and ctms.media_type_code in ('EML','E')
left outer join finance.postal_address pas
	on tmp3.bilt_cust_nbr = pas.customer_num 
	and tmp3.shpto_cust_nbr = pas.customer_num
	and tmp3.shpto_addr_seq_id = pas.address_seq_num 
	and tmp3.so_bu_id = pas.business_unit_id 
	and pas.address_type_code = 'S'
inner join corp_drm_pkg.phys_geo_hier pgh on tmp3.cust_bu_id = pgh.bu_id
where pgh.rgn_abbr = 'AMER'
group by 1,2,3,4,5,6,7,8,9,10
;
delete from tmp_work_db.ev_gbl_cons_nps_tmp4_x 
where cnsld_src_txn_bu_id = 8270 and cnsld_src_txn_id in ( select so_nbr from sas_pulse_Stg.SB_DDW_CN_USERS )
;
insert into tmp_work_db.ev_gbl_cons_nps_tmp4_x
    (
		evnt_subtype_cd,
        cnsld_src_txn_id,
        cnsld_src_txn_bu_id,
        iso_ctry_cd_2,
        iso_lang_cd,
        cust_co_nm,
        frst_nm,
        last_nm,
        full_nm,
        email_addr_id
    )
select
tmp3.evnt_subtype_cd,
tmp3.cnsld_src_txn_id,
tmp3.cnsld_src_txn_bu_id,
cn_sb.iso_ctry_cd_2,
cn_sb.iso_lang_cd,
null as cust_co_nm,
null as frst_nm,
null as last_nm,
cn_sb.full_nm,
cn_sb.email_addr_id
from sas_pulse_stg.ev_gbl_cons_nps_tmp3 tmp3
inner join sas_pulse_Stg.sb_ddw_cn_users cn_sb on tmp3.so_nbr = cn_sb.so_nbr and tmp3.so_bu_id = cn_sb.so_bu_id




/*
--EMEA
select
	tmp3.evnt_subtype_cd,
	tmp3.cnsld_src_txn_id,
	tmp3.cnsld_src_txn_bu_id,
	cast(co.country_code_iso2 as char(2)) as iso_ctry_cd_2,
	cast(pcce.marketing_language_code as char(2)) as iso_lang_cd,
	cast(trim(case when (ce.customer_name is not null or ce.customer_name_2nd_line is not null) 
	     then trim((coalesce(ce.customer_name,' ')) || ' ' || (coalesce(ce.customer_name_2nd_line,' ')))
	     else null end) as varchar(250)) as cust_co_nm,
	cast(trim(cce.first_name) as varchar(100)) as frst_nm,
	cast(trim(cce.last_name) as varchar(100)) as last_nm,
	cast(trim(case when (cce.first_name is not null or cce.last_name is not null) 
			then (trim(coalesce(cce.first_name , '')) || ' ' || trim(coalesce(cce.last_name, ''))) else null end) as varchar(200)) as full_nm,
	cast(trim(case when pcce.email_flag = 'Y' then pcce.contact_email_address else null end) as varchar(254)) as email_addr_id
from sas_pulse_stg.ev_gbl_cons_nps_tmp3 tmp3
left outer join sas_pulse_stg.ev_gbl_cons_nps_cce cce 
	on tmp3.bilt_addr_seq_id = cce.address_seq_num 
	and tmp3.bilt_cntct_id = cce.customer_contact_seq_num 
	and tmp3.cust_bu_id = cce.business_unit_id
left outer join 
	(select business_unit_id, 
			order_num, 
			bill_to_pros_contact_id, 
			ship_to_pros_contact_id
			QUALIFY RANK() OVER (PARTITION BY business_unit_id, order_num ORDER BY ORDER_DATE DESC) = 1
	  from euro_rt.quote_euro) qh 
	on tmp3.cnsld_src_txn_id = qh.order_num 
	and tmp3.cnsld_src_txn_bu_id = qh.business_unit_id
left outer join euro_rt.prospect_cust_contact_euro pcce 
	on qh.bill_to_pros_contact_id = pcce.prospect_contact_id
	and qh.business_unit_id = pcce.business_unit_id
left outer join euro_fin_mart.customer_euro ce
	on tmp3.bilt_cust_nbr = ce.customer_num 
	and tmp3.cust_bu_id = ce.business_unit_id
inner join corp_drm_pkg.phys_geo_hier pgh on tmp3.cust_bu_id = pgh.bu_id
left outer join corp.country co
	on ce.country_code = co.country_code
where pgh.rgn_abbr = 'EMEA'
group by 1,2,3,4,5,6,7,8,9,10


union

--APJ
SELECT
	tmp3.evnt_subtype_cd,
	tmp3.cnsld_src_txn_id,
	tmp3.cnsld_src_txn_bu_id,
	CAST(COALESCE(pgh.iso_ctry_code_2, (CASE 
		WHEN bc.elec_addr IS NOT NULL THEN bc.ctry_cd
		WHEN sc.elec_addr IS NOT NULL THEN sc.ctry_cd
		ELSE NULL END)) AS VARCHAR(2)) AS iso_ctry_2_cd,
	CAST(NULL AS VARCHAR(2)) AS iso_lang_cd,	
	CAST(TRIM(CASE 
		WHEN bc.elec_addr IS NOT NULL THEN COALESCE(bcu.cust_nm, bc.cust_nm)
		WHEN sc.elec_addr IS NOT NULL THEN COALESCE(scu.cust_nm, sc.cust_nm)
		ELSE NULL END) AS VARCHAR(250)) AS cust_co_nm,	
	CAST(TRIM(CASE 
		WHEN bc.elec_addr IS NOT NULL THEN bc.frst_nm
		WHEN sc.elec_addr IS NOT NULL THEN sc.frst_nm
		ELSE NULL END) AS VARCHAR(100)) AS x_frst_nm,
	CAST(TRIM(CASE 
		WHEN bc.elec_addr IS NOT NULL THEN bc.last_nm
		WHEN sc.elec_addr IS NOT NULL THEN sc.last_nm
		ELSE NULL END) AS VARCHAR(100)) AS x_last_nm,
	CAST(TRIM(CASE
		WHEN x_frst_nm IS NOT NULL OR x_last_nm IS NOT NULL THEN COALESCE(x_frst_nm,'') || ' ' || COALESCE(x_last_nm,'')
		ELSE NULL END) AS VARCHAR(200)) AS full_nm,
	CAST(TRIM(COALESCE(bc.elec_addr, sc.elec_addr)) AS VARCHAR(254)) AS email_addr_id
FROM sas_pulse_stg.ev_gbl_cons_nps_tmp3 tmp3
LEFT OUTER JOIN party_pkg.sls_svc_cust bcu ON tmp3.bilt_src_cust_id = bcu.src_cust_id	AND tmp3.cnsld_src_txn_bu_id = bcu.src_bu_id	
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
			elec_addr
			QUALIFY RANK() OVER (PARTITION BY  src_cust_id ORDER BY elec_addr DESC, DW_INS_UPD_DTS DESC, COALESCE(PRIM_EMAIL_FLG, 'N') DESC,  cust_nm DESC, last_nm DESC) = 1
		 FROM party_pkg.sls_svc_cust_ph_cntct_bridge
		 WHERE addr_cntct_role_cd = 'BILL_TO' AND elec_addr IS NOT NULL 	
		 GROUP BY
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
			DW_INS_UPD_DTS,
			PRIM_EMAIL_FLG )  bc 	
ON tmp3.cnsld_src_txn_bu_id = bc.src_bu_id AND tmp3.bilt_src_cust_id = bc.src_cust_id AND tmp3.bilt_addr_seq_id = bc.src_postal_addr_id  AND tmp3.bilt_cntct_id = bc.src_cntct_id		
LEFT OUTER JOIN party_pkg.sls_svc_cust scu ON tmp3.shpto_src_cust_id = scu.src_cust_id	AND tmp3.cnsld_src_txn_bu_id = scu.src_bu_id	
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
			elec_addr
			QUALIFY RANK() OVER (PARTITION BY src_cust_id ORDER BY elec_addr DESC, DW_INS_UPD_DTS DESC, COALESCE(PRIM_EMAIL_FLG, 'N')  DESC, cust_nm DESC, last_nm DESC ) = 1
		 FROM party_pkg.sls_svc_cust_ph_cntct_bridge
		 WHERE addr_cntct_role_cd = 'SHIP_TO' AND elec_addr IS NOT NULL 	
		 GROUP BY
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
			DW_INS_UPD_DTS,
			PRIM_EMAIL_FLG)  sc 	
ON tmp3.cnsld_src_txn_bu_id = sc.src_bu_id AND tmp3.shpto_src_cust_id = sc.src_cust_id AND tmp3.shpto_addr_seq_id = sc.src_postal_addr_id  AND tmp3.shpto_cntct_id = sc.src_cntct_id		
INNER JOIN corp_drm_pkg.phys_geo_hier pgh ON tmp3.cust_bu_id = pgh.bu_id
WHERE pgh.rgn_abbr = 'APJ' 
GROUP BY 1,2,3,4,5,6,7,8,9,10;
*/

--00:01:25	3409926