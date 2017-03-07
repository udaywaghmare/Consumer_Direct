COLLECT STATISTICS sas_pulse_stg.ev_gbl_cons_nps_tmp3 COLUMN (SO_NBR ,SO_BU_ID);
COLLECT SUMMARY STATISTICS ON sas_pulse_stg.ev_gbl_cons_nps_tmp5;
COLLECT SUMMARY STATISTICS ON sas_pulse_stg.ev_gbl_cons_nps_tmp4;

Select Count(1) From sas_pulse_stg.ev_gbl_cons_nps_tmp6_prev_q; --4219522
Select Count(1) From sas_pulse_stg.ev_gbl_cons_nps_tmp6; --3958042

Delete  sas_pulse_stg.ev_gbl_cons_nps_tmp6_prev_q All;
Insert into sas_pulse_stg.ev_gbl_cons_nps_tmp6_prev_q
Select * From sas_pulse_stg.ev_gbl_cons_nps_tmp6;

DELETE FROM sas_pulse_stg.ev_gbl_cons_nps_tmp6 ALL;
insert into sas_pulse_stg.ev_gbl_cons_nps_tmp6
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
	acct_id,
	acct_co_nm,
	cust_bu_id,
	cust_nbr,
	cust_lcl_chnl_cd,
	cust_co_nm,
	iso_ctry_cd_2,
	iso_lang_cd,
	frst_nm,
	last_nm,
	full_nm,
	email_addr_id,
	SYS_COMB_HIER_CD,
	SNP_COMB_HIER_CD
)
select	
	tmp3.evnt_ldr_id,
	tmp3.evnt_type_cd,
	tmp3.evnt_subtype_cd,
	tmp3.evnt_dt,
	tmp3.evnt_dts,
	tmp3.cnsld_src_txn_id,
	tmp3.cnsld_src_txn_bu_id,
	tmp3.so_nbr,
	tmp3.so_bu_id,
	tmp3.so_lcl_chnl_cd,
	tmp3.so_dt,
	tmp3.so_inv_dt,
	tmp3.qte_nbr,
	tmp3.qte_bu_id,
	tmp3.qte_lcl_chnl_cd,
	tmp3.qte_dt,
	tmp3.prim_sls_assoc_nbr,
	tmp3.secnd_sls_assoc_nbr,
	tmp3.sys_itm_cls_nm,
	tmp3.sys_flg,
	tmp3.snp_flg,
	tmp3.svcs_flg,
	tmp3.sls_chnl_cd,
	tmp3.tot_revn_disc_amt,
	tmp5.acct_id,
	tmp5.acct_co_nm,
	tmp3.cust_bu_id,
	tmp3.bilt_cust_nbr as cust_nbr,
	tmp3.cust_lcl_chnl_cd,
	tmp4.cust_co_nm,
	coalesce(tmp4.iso_ctry_cd_2, tmp5.iso_ctry_cd_2) as iso_ctry_cd_2,
	coalesce(tmp4.iso_lang_cd, tmp5.iso_lang_cd) as iso_lang_cd,
	case when tmp4.full_nm is null then tmp5.frst_nm else tmp4.frst_nm end as frst_nm,
	case when tmp4.full_nm is null then tmp5.last_nm else tmp4.last_nm end as last_nm,
	coalesce(tmp4.full_nm, tmp5.full_nm) as full_nm,
	coalesce(tmp4.email_addr_id, tmp5.email_addr_id, em.email_addr_id) as email_addr_id,
	tmp3.SYS_COMB_HIER_CD,
	tmp3.SNP_COMB_HIER_CD
from sas_pulse_stg.ev_gbl_cons_nps_tmp3 tmp3
left outer join sas_pulse_stg.ev_gbl_cons_nps_tmp4 tmp4
	on tmp3.evnt_subtype_cd = tmp4.evnt_subtype_cd
	and tmp3.cnsld_src_txn_id = tmp4.cnsld_src_txn_id
	and tmp3.cnsld_src_txn_bu_id = tmp4.cnsld_src_txn_bu_id
left outer join sas_pulse_stg.ev_gbl_cons_nps_tmp5 tmp5
	on tmp3.evnt_subtype_cd = tmp5.evnt_subtype_cd
	and tmp3.cnsld_src_txn_id = tmp5.cnsld_src_txn_id
	and tmp3.cnsld_src_txn_bu_id = tmp5.cnsld_src_txn_bu_id
left outer join
	(select
		tmp3.so_nbr,
		tmp3.so_bu_id,
		trim(coalesce(em1.email_addr, em2.email_addr, em3.email_addr)) as email_addr_id
	from
	sas_pulse_stg.ev_gbl_cons_nps_tmp3 tmp3
	left outer join
		(select
			so_nbr,
			so_bu_id,
			max(case when email_address like '%dell.com%' then null else email_address end) as email_addr
		from
		(select
			tmp3.so_nbr,
			tmp3.so_bu_id,
			case when char2hexint(en.email_address) like '000000%' then substring(en.email_address from 2)
				else en.email_address end as email_address
		from 
			(select tmp.so_bu_id, tmp.so_nbr, coalesce(ohe.gedis_order_num, tmp.so_nbr) as alt_ord_nbr 
			from sas_pulse_stg.ev_gbl_cons_nps_tmp3 tmp
			left outer join euro_fin.order_header_all_euro ohe
				on tmp.so_nbr = ohe.order_num
				and tmp.so_bu_id = ohe.business_unit_id) tmp3
		inner join finance.email_notification en
			on tmp3.alt_ord_nbr = en.source_system_trans_id 
			and tmp3.so_bu_id = en.business_unit_id
		where en.recipient_type_code = 'TO'
			and en.src_sys_trans_attribute = 'order_num'
			and en.notification_status_code not in ('bounced','failed')
			and en.notification_create_dts = 
				(select max(notification_create_dts) 
				from finance.email_notification 
				where source_system_trans_id = en.source_system_trans_id
					and business_unit_id = en.business_unit_id
					and src_sys_trans_attribute = 'order_num')) a
		group by 1,2)  em1 
		on tmp3.so_nbr = em1.so_nbr
		and tmp3.so_bu_id = em1.so_bu_id
	left outer join
		(select
			tmp3.so_nbr,
			tmp3.so_bu_id,
			max(case when cmd.email_address like '%@dell.com%' then null else cmd.email_address end) as email_addr
		from (select tmp.so_bu_id, tmp.so_nbr, coalesce(ohe.gedis_order_num, tmp.so_nbr) as alt_ord_nbr from sas_pulse_stg.ev_gbl_cons_nps_tmp3 tmp
		left outer join euro_fin.order_header_all_euro ohe
			on tmp.so_nbr = ohe.order_num
			and tmp.so_bu_id = ohe.business_unit_id) tmp3
		inner join us_fin.customer_message cmm
			on tmp3.alt_ord_nbr = cmm.source_item_id 
			and tmp3.so_bu_id = cmm.business_unit_id
		inner join us_fin.customer_message_detail cmd
			on cmm.customer_message_id = cmd.customer_message_id
		where cmd.recipient_type_code = 'TO'
			and cmm.action_item_id in (1, 3) 
			and cmd.message_status_id not in (4,5,6,7,8)
			and cmm.creation_date = 
				(select max(creation_date) 
				from us_fin.customer_message 
				where source_item_id = cmm.source_item_id
					and business_unit_id = cmm.business_unit_id)
		group by 1,2) em2
		on tmp3.so_nbr = em2.so_nbr
		and tmp3.so_bu_id = em2.so_bu_id 
	left outer join
		(select
			tmp3.so_nbr,
			tmp3.so_bu_id,
			max(case when oq.emailfax like '%@dell.com%' then null else oq.emailfax end) as email_addr
		from sas_pulse_stg.ev_gbl_cons_nps_tmp3 tmp3
		inner join us_fin.outbound_quote oq
			on tmp3.so_nbr = oq.quote_num 
			and tmp3.so_bu_id = oq.business_unit
		inner join us_fin.outbound o
			on oq.outbound_id = o.outbound_id  
			and oq.business_unit = o.business_unit
			and o.outbound_type_code ='OR'
			and o.poll_status = 'Sent'
		where oq.date_sent_from_server = 
				(select max(date_sent_from_server) 
				from us_fin.outbound_quote 
				where quote_num = oq.quote_num
					and business_unit = oq.business_unit)
		group by 1,2) em3
		on tmp3.so_nbr = em3.so_nbr
		and tmp3.so_bu_id = em3.so_bu_id) em  /* get additional email address values from front end sources */
	on tmp3.so_nbr = em.so_nbr
	and tmp3.so_bu_id = em.so_bu_id;


--00:00:40	3409926

 