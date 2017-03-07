/*
*/

DEL FROM sas_pulse_stg.ev_gbl_cons_nps_tmp5_qh ALL;
INSERT INTO sas_pulse_stg.ev_gbl_cons_nps_tmp5_qh
SELECT business_unit_id, 
order_num, 
bill_to_pros_contact_id, 
ship_to_pros_contact_id
QUALIFY RANK() OVER (PARTITION BY business_unit_id, order_num ORDER BY ORDER_DATE DESC) = 1
FROM euro_rt.quote_euro
;
COLLECT STATISTICS COLUMN (bill_to_pros_contact_id) ON  sas_pulse_stg.ev_gbl_cons_nps_tmp5_qh;
DELETE FROM sas_pulse_stg.ev_gbl_cons_nps_person_profile ALL;
INSERT INTO sas_pulse_stg.ev_gbl_cons_nps_person_profile 
SEL person_first_name,
person_last_name,
party_id
FROM finance.person_profile
GROUP BY 1,2,3
;

DELETE FROM sas_pulse_stg.ev_gbl_cons_nps_party_rel ALL;
INSERT INTO sas_pulse_stg.ev_gbl_cons_nps_party_rel 
SEL 
subject_id,
party_id,
object_id
FROM finance.party_relationship
WHERE relationship_type_code = 'EMPLOYMENT'
GROUP BY 1,2,3
;
DEL FROM sas_pulse_stg.ev_gbl_cons_nps_tmp5_PCP ALL;
INSERT INTO sas_pulse_stg.ev_gbl_cons_nps_tmp5_PCP 
SELECT 
party_id,
email_address,
accepts_marcom_flag
QUALIFY RANK() OVER (PARTITION BY party_id ORDER BY load_seq_num DESC, contact_point_id DESC) = 1
FROM finance.contact_point /* choose max contact_point_id with most recent load_seq_num */
WHERE contact_point_type_code = 'EMAIL' AND status = 'A'
;

COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN (SO_BU_ID);
COLLECT STATISTICS COLUMN (EVNT_SUBTYPE_CD ,CNSLD_SRC_TXN_ID , CNSLD_SRC_TXN_BU_ID) ON sas_pulse_stg.ev_gbl_cons_nps_tmp3;
COLLECT STATISTICS COLUMN (CNSLD_SRC_TXN_ID,CNSLD_SRC_TXN_BU_ID) ON sas_pulse_stg.ev_gbl_cons_nps_tmp3;
COLLECT STATISTICS COLUMN (PARTY_ID) ON sas_pulse_stg.ev_gbl_cons_nps_tmp5_PCP;
COLLECT STATISTICS COLUMN (PARTY_ID) ON sas_pulse_stg.ev_gbl_cons_nps_person_profile;

Select Count(1) From  sas_pulse_stg.ev_gbl_cons_nps_tmp5_prev_q;--3645265
Select Count(1) From  sas_pulse_stg.ev_gbl_cons_nps_tmp5; --3409875

delete from sas_pulse_stg.ev_gbl_cons_nps_tmp5_prev_q All;
Insert into sas_pulse_stg.ev_gbl_cons_nps_tmp5_prev_q
Select * From sas_pulse_stg.ev_gbl_cons_nps_tmp5;
delete from sas_pulse_stg.ev_gbl_cons_nps_tmp5;


INSERT INTO sas_pulse_stg.ev_gbl_cons_nps_tmp5
(
	evnt_subtype_cd,
	cnsld_src_txn_id,
	cnsld_src_txn_bu_id,
	iso_ctry_cd_2,
	iso_lang_cd,
	acct_id,
	acct_co_nm,
	frst_nm,
	last_nm,
	full_nm,
	email_addr_id
)
/*AMER*/
/*
select
	a.evnt_subtype_cd,
	a.cnsld_src_txn_id,
	a.cnsld_src_txn_bu_id,
	a.iso_ctry_cd_2,
	a.iso_lang_cd,
	a.acct_id,
	a.acct_co_nm,
	a.frst_nm,
	a.last_nm,
	a.full_nm,
	a.email_addr_id
qualify rank() over (partition by a.evnt_subtype_cd, a.cnsld_src_txn_id, a.cnsld_src_txn_bu_id 
	order by a.acct_id, a.acct_co_nm, a.frst_nm, a.last_nm, a.full_nm, a.email_addr_id, a.iso_ctry_cd_2, a.iso_lang_cd) = 1
from
(select
		tmp3.evnt_subtype_cd,
		tmp3.cnsld_src_txn_id,
		tmp3.cnsld_src_txn_bu_id,
		coalesce(loc.country_code,pgh.iso_ctry_code_2) as iso_ctry_cd_2,
		case when tmp3.so_bu_id = 707 and loc.state_prov_code = 'QC' then 'FR' else null end as iso_lang_cd,
		ca.party_id as acct_id,
		trim(ca.account_name) as acct_co_nm,
		trim(coalesce(pp.person_first_name, rpp.person_first_name)) as frst_nm,
		trim(coalesce(pp.person_last_name, rpp.person_last_name)) as last_nm,
		trim(case when (frst_nm is not null or last_nm is not null) 
				then (trim(coalesce(frst_nm, '')) || ' ' || trim(coalesce(last_nm, ''))) else null end) as full_nm,
		max(trim(cast(pcp.email_address as varchar(254)))) as email_addr_id
from  sas_pulse_stg.ev_gbl_cons_nps_tmp3 tmp3
inner join finance.customer_account ca on tmp3.bilt_cust_nbr = ca.customer_num and tmp3.so_bu_id = ca.business_unit_id
left outer join finance.customer_account_site cas on ca.customer_account_id = cas.customer_account_id 
		and TRIM(TRAILING '.' FROM  CAST(tmp3.bilt_addr_seq_id AS VARCHAR(20))) = TRIM(TRAILING '.' FROM CAST(cas.address_seq_num    AS VARCHAR(20)))
		and cas.address_type_code ='B'
left outer join finance.party_site ps on cas.party_site_id = ps.party_site_id and ps.status = 'A'
left outer join finance.location loc on ps.location_id = loc.location_id
left outer join finance.party p on ps.party_id = p.party_id	
left outer join sas_pulse_stg.ev_gbl_cons_nps_person_profile pp on p.party_id = pp.party_id
left outer join sas_pulse_stg.ev_gbl_cons_nps_party_rel pr on ps.party_id = pr.party_id
left outer join finance.party rp on pr.object_id = rp.party_id and rp.party_type_code = 'ORGANIZATION'
left outer join sas_pulse_stg.ev_gbl_cons_nps_person_profile rpp on pr.subject_id = rpp.party_id
left outer join sas_pulse_stg.ev_gbl_cons_nps_tmp5_PCP pcp ON p.party_id = pcp.party_id
inner join corp_drm_pkg.phys_geo_hier pgh on tmp3.cust_bu_id = pgh.bu_id
where pgh.rgn_abbr = 'AMER' and 
ca.customer_account_id = (select max(customer_account_id) from finance.customer_account  where ca.customer_num = customer_num and ca.business_unit_id = business_unit_id)
group by 1,2,3,4,5,6,7,8,9,10
) a

union
*/
/*EMEA*/
SELECT
	b.evnt_subtype_cd,
	b.cnsld_src_txn_id,
	b.cnsld_src_txn_bu_id,
	b.iso_ctry_cd_2,
	b.iso_lang_cd,
	b.acct_id,
	b.acct_co_nm,
	b.frst_nm,
	b.last_nm,
	b.full_nm,
	b.email_addr_id
QUALIFY RANK() OVER (PARTITION BY b.evnt_subtype_cd, b.cnsld_src_txn_id, b.cnsld_src_txn_bu_id 
	ORDER BY b.acct_id, b.acct_co_nm, b.frst_nm, b.last_nm, b.full_nm, b.email_addr_id, b.iso_ctry_cd_2, b.iso_lang_cd) = 1
FROM
	(SELECT
		tmp3.evnt_subtype_cd,
		tmp3.cnsld_src_txn_id,
		tmp3.cnsld_src_txn_bu_id,
		loc.country_code AS iso_ctry_cd_2,
		p.language_code AS iso_lang_cd,
		ca.party_id AS acct_id,
		TRIM(ca.account_name) AS acct_co_nm,
		TRIM(COALESCE(pp.person_first_name, rpp.person_first_name)) AS frst_nm,
		TRIM(COALESCE(pp.person_last_name, rpp.person_last_name)) AS last_nm,
		TRIM(CASE WHEN (frst_nm IS NOT NULL OR last_nm IS NOT NULL) 
			THEN (TRIM(COALESCE(frst_nm, '')) || ' ' || TRIM(COALESCE(last_nm, ''))) ELSE NULL end) AS full_nm,
		MAX(CASE WHEN pcp.accepts_marcom_flag = 'Y' THEN TRIM(CAST(pcp.email_address AS VARCHAR(254))) ELSE NULL end) AS email_addr_id
from  sas_pulse_stg.ev_gbl_cons_nps_tmp3 tmp3
inner join finance.customer_account ca
		ON tmp3.bilt_cust_nbr = ca.dw_omega_customer_number 
		AND tmp3.cust_bu_id = ca.business_unit_id
left outer join sas_pulse_stg.ev_gbl_cons_nps_tmp5_qh qh 
	ON tmp3.cnsld_src_txn_id = qh.order_num 
	AND tmp3.cnsld_src_txn_bu_id = qh.business_unit_id
LEFT OUTER JOIN euro_rt.prospect_cust_contact_euro pcce 
	ON qh.bill_to_pros_contact_id = pcce.prospect_contact_id
	AND qh.business_unit_id = pcce.business_unit_id
left outer join finance.customer_account_site cas
		ON cas.customer_account_id = ca.customer_account_id 				
		AND TRIM(TRAILING '.' FROM  CAST(pcce.prospect_address_id AS VARCHAR(20))) = TRIM(TRAILING '.' FROM CAST(cas.address_seq_num    AS VARCHAR(20)))
		--and pcce.prospect_address_id = cas.address_seq_num				
		AND pcce.prospect_contact_id = cas.contact_id				
		AND cas.address_type_code = 'B'
left outer join finance.party_site ps
		ON cas.party_site_id = ps.party_site_id	
		AND ps.status = 'A'
left outer join finance.location loc
		ON ps.location_id = loc.location_id
left outer join finance.party p
		ON ps.party_id = p.party_id	
left outer join sas_pulse_stg.ev_gbl_cons_nps_person_profile pp
		ON p.party_id = pp.party_id
left outer join sas_pulse_stg.ev_gbl_cons_nps_party_rel pr
		ON ps.party_id = pr.party_id
		--and pr.relationship_type_code = 'EMPLOYMENT'	
left outer join finance.party rp
		ON pr.object_id = rp.party_id
		AND rp.party_type_code = 'ORGANIZATION'
left outer join sas_pulse_stg.ev_gbl_cons_nps_person_profile rpp
		ON pr.subject_id = rpp.party_id
left outer join sas_pulse_stg.ev_gbl_cons_nps_tmp5_PCP pcp
		ON p.party_id = pcp.party_id
inner join corp_drm_pkg.phys_geo_hier pgh ON tmp3.cust_bu_id = pgh.bu_id
	WHERE pgh.rgn_abbr = 'EMEA' 
		AND ca.customer_account_id = (SELECT MAX(customer_account_id) 
									  FROM finance.customer_account
									  WHERE ca.customer_num = customer_num
											AND ca.business_unit_id = business_unit_id)
	GROUP BY 1,2,3,4,5,6,7,8,9,10) b;
