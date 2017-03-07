select 	ord_type_cd	, count(*) from 	 sls_pkg.so_hdr_fact 		group by 1 order by 1	;
select 	ord_inv_ind	, count(*) from 	 sls_pkg.so_hdr_fact 		group by 1 order by 1	;
select 	ord_dspsn_stat_cd	, count(*) from 	 sls_pkg.so_hdr_fact 		group by 1 order by 1	;
select 	tmzn_loc_id	, count(*) from 	 sls_pkg.so_hdr_fact 		group by 1 order by 1	;

	
select case when ord_dt  BETWEEN '2016-01-01' AND '2016-03-01' then 2016 else 2015 end as daterange, ord_type_cd, count(*)
from 	 sls_pkg.so_hdr_fact 
where ord_dt  BETWEEN '2015-01-01' AND '2015-03-01' or  ord_dt  BETWEEN '2016-01-01' AND '2016-03-01' 
group by 1,2
	
select case when ord_dt  BETWEEN '2016-01-01' AND '2016-03-01' then 2016 else 2015 end as daterange, ord_inv_ind, count(*)
from 	 sls_pkg.so_hdr_fact 
where ord_dt  BETWEEN '2015-01-01' AND '2015-03-01' or  ord_dt  BETWEEN '2016-01-01' AND '2016-03-01' 
group by 1,2
	
select case when ord_dt  BETWEEN '2016-01-01' AND '2016-03-01' then 2016 else 2015 end as daterange, ord_dspsn_stat_cd, count(*)
from 	 sls_pkg.so_hdr_fact 
where ord_dt  BETWEEN '2015-01-01' AND '2015-03-01' or  ord_dt  BETWEEN '2016-01-01' AND '2016-03-01' 
group by 1,2
	
select case when ord_dt  BETWEEN '2016-01-01' AND '2016-03-01' then 2016 else 2015 end as daterange, tmzn_loc_id, count(*)
from 	 sls_pkg.so_hdr_fact 
where ord_dt  BETWEEN '2015-01-01' AND '2015-03-01' or  ord_dt  BETWEEN '2016-01-01' AND '2016-03-01' 
group by 1,2

	 


sel 
	case when odf.ord_dt  BETWEEN '2015-02-01' AND '2016-02-01' then 2016 else 2015 end as daterange, 
	
	ch.CUST_TYPE_CODE, 
	ch.SEG_CODE, 
	ch.BU_ID , 
	ch.LCL_CHNL_CODE,
	pgh.ISO_CTRY_CODE_2,
	cast(m.fiscal_year as varchar(10)) || '-' || cast(m.fiscal_month as varchar(10))  AS ord_dt_month,
--	cph.BRAND_CATG_DESC,
	CASE WHEN COALESCE(shf.onln_lvl_cd, shf.intrnt_revn_cd,'N') IN ('2','N') THEN 'OFFLINE' ELSE 'ONLINE' END AS sls_chnl_cd,
	SUM(odf.revn_disc_txn_amt *(CASE WHEN odf.revn_usd_rt IS NULL OR odf.revn_usd_rt = 0 THEN 1 ELSE odf.revn_usd_rt END)) AS tot_revn_disc_amt,
	COUNT(*) AS n
FROM sls_pkg.so_hdr_fact shf	
INNER JOIN sls_pkg.so_dtl_fact odf ON shf.ORD_NBR = odf.ORD_NBR AND shf.SRC_BU_ID = odf.SRC_BU_ID
inner join CORP.FISCAL_MONTH_INFO m on odf.ord_dt BETWEEN m.BEGIN_DATE AND m.END_DATE
INNER JOIN itm_pkg.comb_prod_hier cph 		
	ON	odf.extrnl_comb_hier_cd = cph.comb_hier_cd
INNER JOIN corp_drm_pkg.phys_geo_hier pgh on shf.ref_bu_id = pgh.bu_id
INNER JOIN corp_drm_pkg.chnl_hier ch ON shf.ref_bu_id = ch.bu_id		
	AND shf.ref_lcl_chnl_cd = ch.lcl_chnl_code	
where 
	shf.ord_type_cd = 'I'
	AND shf.ord_inv_ind = 'A'
	AND shf.ord_dspsn_stat_cd = 'CLOSED'
	AND ( odf.ord_dt  BETWEEN '2014-02-01' AND '2015-02-01' or  odf.ord_dt  BETWEEN '2015-02-01' AND '2016-02-01' )
	AND pgh.ISO_CTRY_CODE_2 in ('US','CA','BR','MX','GB','FR','DE','JP','CN','IN','AU') 
	AND cph.BRAND_CATG_DESC NOT IN ('UNKNOWN', 'SPARES')	
	AND odf.tmzn_loc_id IN (1,2,3,4,5) 
	AND shf.tmzn_loc_id IN (1,2,3,4,5)
group by 1,2,3,4,5,6,7,8
having tot_revn_disc_amt > 0

;


sel 
	case when odf.ord_dt  BETWEEN '2015-02-01' AND '2016-02-01' then 2016 else 2015 end as daterange, 
	ch.CUST_TYPE_CODE, 
	ch.SEG_CODE, 
	ch.BU_ID , 
	ch.LCL_CHNL_CODE,
	pgh.ISO_CTRY_CODE_2,
	m.fiscal_year ,
	m.fiscal_month ,
--	cph.BRAND_CATG_DESC,
	CASE WHEN COALESCE(shf.onln_lvl_cd, shf.intrnt_revn_cd,'N') IN ('2','N') THEN 'OFFLINE' ELSE 'ONLINE' END AS sls_chnl_cd,
	SUM(odf.revn_disc_txn_amt *(CASE WHEN odf.revn_usd_rt IS NULL OR odf.revn_usd_rt = 0 THEN 1 ELSE odf.revn_usd_rt END)) AS tot_revn_disc_amt,
	COUNT(*) AS n
FROM sls_pkg.so_hdr_fact shf	
INNER JOIN sls_pkg.so_dtl_fact odf ON shf.ORD_NBR = odf.ORD_NBR AND shf.SRC_BU_ID = odf.SRC_BU_ID
inner join ( select * from CORP.FISCAL_MONTH_INFO where fiscal_year between 2014 and 2017) as m on odf.ord_dt >= m.BEGIN_DATE AND odf.ord_dt <= m.END_DATE
INNER JOIN itm_pkg.comb_prod_hier cph 		
	ON	odf.extrnl_comb_hier_cd = cph.comb_hier_cd
INNER JOIN corp_drm_pkg.phys_geo_hier pgh on shf.ref_bu_id = pgh.bu_id
INNER JOIN corp_drm_pkg.chnl_hier ch ON shf.ref_bu_id = ch.bu_id		
	AND shf.ref_lcl_chnl_cd = ch.lcl_chnl_code	
where 
	shf.ord_type_cd = 'I'
	AND shf.ord_inv_ind = 'A'
	AND shf.ord_dspsn_stat_cd = 'CLOSED'
	AND ( odf.ord_dt  BETWEEN '2014-02-01' AND '2015-02-01' or  odf.ord_dt  BETWEEN '2015-02-01' AND '2016-02-01' )
	AND pgh.ISO_CTRY_CODE_2 in ('US','CA','BR','MX','GB','FR','DE','JP','CN','IN','AU') 
	AND cph.BRAND_CATG_DESC NOT IN ('UNKNOWN', 'SPARES')	
	AND odf.tmzn_loc_id IN (1,2,3,4,5) 
	AND shf.tmzn_loc_id IN (1,2,3,4,5)
group by 1,2,3,4,5,6,7,8,9
having tot_revn_disc_amt > 0
;
 