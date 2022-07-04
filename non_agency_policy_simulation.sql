---- 1) Driver table 
--willapay-data-staging.dev_eg_adhocs.invoice_model_scores 

DECLARE start_date DATE DEFAULT DATE('2021-01-01') ;
DECLARE asof_date DATE DEFAULT DATE('2021-01-01') ;
DECLARE end_date DATE DEFAULT DATE('2021-01-01');

set start_date = DATE('2022-02-01');
set end_date = DATE('2022-02-28');
set asof_date = DATE('2022-02-01');
/*
create or replace table willapay-data-staging.dev_vs_workspace.daily_dates as (
  SELECT day
FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE('2021-01-01'), CURRENT_DATE(), INTERVAL 1 DAY)
) AS day

);

*/

create or replace table willapay-data-staging.dev_vs_workspace.recent_trans
 as (
    select *
    from `willapay-data-production.marts.dim_payment_requests`
    where transaction_id not in (144890, 594, 805, 52996, 200161, 200489, 491, 783, 143445)
    and dbt_valid_to is null
   and DATE(created_at)  > '2021-04-01' 
);


create or replace table   willapay-data-staging.dev_vs_workspace.recent_trans_dq_flags as(
    select *, 
    ifnull(date_diff(cast(paid_at as date), cast(created_at as date), day),(date_diff(current_date,cast(created_at as date),day)))  as dpc,
    ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(current_date,due_date,day))) as dpd,


    
    (case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(current_date,due_date,day))) >= 1 then 1 else 0 end) as dpd1,
    (case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(current_date,due_date,day))) >= 7 then 1 else 0 end) as dpd7,
    (case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(current_date,due_date,day))) >= 30 then 1 else 0 end) as dpd30,
    (case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(current_date,due_date,day))) >= 45 then 1 else 0 end) as dpd45,
    (case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(current_date,due_date,day))) >= 60 then 1 else 0 end) as dpd60,
    (case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(current_date,due_date,day))) >= 90 then 1 else 0 end) as dpd90,
    (case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(current_date,due_date,day))) >= 120 then 1 else 0 end) as dpd120,

    (case when ifnull(date_diff(cast(paid_at as date), cast(created_at as date), day),(date_diff(current_date,cast(created_at as date),day))) >= 1 then 1 else 0 end) as dpc1,
    (case when ifnull(date_diff(cast(paid_at as date), cast(created_at as date), day),(date_diff(current_date,cast(created_at as date),day))) >= 7 then 1 else 0 end) as dpc7,
    (case when ifnull(date_diff(cast(paid_at as date), cast(created_at as date), day),(date_diff(current_date,cast(created_at as date),day))) >= 30 then 1 else 0 end) as dpc30,
    (case when ifnull(date_diff(cast(paid_at as date), cast(created_at as date), day),(date_diff(current_date,cast(created_at as date),day))) >= 45 then 1 else 0 end) as dpc45,
    (case when ifnull(date_diff(cast(paid_at as date), cast(created_at as date), day),(date_diff(current_date,cast(created_at as date),day))) >= 60 then 1 else 0 end) as dpc60,
    (case when ifnull(date_diff(cast(paid_at as date), cast(created_at as date), day),(date_diff(current_date,cast(created_at as date),day))) >= 90 then 1 else 0 end) as dpc90,
    (case when ifnull(date_diff(cast(paid_at as date), cast(created_at as date), day),(date_diff(current_date,cast(created_at as date),day))) >= 120 then 1 else 0 end) as dpc120,

    date_trunc(due_date, week(sunday)) as truncated_date,
    --date_trunc(due_date, month) as truncated_date,
    (case when split(payer_email_address, '@')[offset(1)] in (
        "relatable.me",
        "corneliacreative.com",
        "hermanaagency.com",
        "hashoff.com",
        "jessegraves.com",
        "slyvm.com",
        "sosaniagency.com",
        "sociablesociety",
        "key-mgmt.com",
        "v7international.com",
        "otterinfluence.com",
        "jessgraves.com",
        "jrichent.com",
        "lstagency.com",
        "streamstrudel.com",
        "kyra.com",
        "playbackai.com",
        "grail-talent.com",
        "thesociablesociety.com",
        "mypeopleknow",
        "thecultureclub.us") 
        then 1 else 0 end) as is_agency_invoice,
    from willapay-data-staging.dev_vs_workspace.recent_trans
    where payer_email_address <> 'appstore@willapay.com'
    and date(created_at) > '2021-04-01'
    --and date(created_at) BETWEEN '2021-01-01' AND   date_sub(asof_date, INTERVAL 75 DAY)
    order by transaction_id
);



create or replace table willapay-data-staging.dev_vs_workspace.partner_daily as (
select a.partner_id, b.day as snap_dt
from willapay-data-staging.dev_vs_workspace.recent_trans_dq_flags a
cross join willapay-data-staging.dev_vs_workspace.daily_dates b
group by 1,2);


create or replace table willapay-data-staging.dev_vs_workspace.partner_daily_1 as (
  select a.partner_id as partner_id_1, a.snap_dt, b.* , ifnull(b.amount_requested_cent,0)/100 as payment_request_dollar
  from willapay-data-staging.dev_vs_workspace.partner_daily a
  left join willapay-data-staging.dev_vs_workspace.recent_trans_dq_flags b
  on a.partner_id = b.partner_id
  and date_sub(a.snap_dt, INTERVAL 65 DAY)  = date(b.created_at)
  --and a.snap_dt  = date_sub(date(b.created_at), INTERVAL 65 DAY)
);



create or replace table willapay-data-staging.dev_vs_workspace.partner_daily_2 as (
select partner_id_1
, snap_dt
, date(created_at) as created_at
, payment_request_id 
, payment_request_dollar as amt_requrested
, sum(case when is_closed = False then 1 else 0 end) over (partition by partner_id_1 order by snap_dt) as un_closed_pr
, sum(payment_request_dollar) over (partition by partner_id_1 order by snap_dt) as total_amt_requestd
, sum(case when is_closed = True then payment_request_dollar else 0 end) over (partition by partner_id_1 order by snap_dt) as total_amt_closed
, sum(case when is_paid = True then 0 else 0 end) over (partition by partner_id_1 order by snap_dt) as total_amt_paid
, sum(case when dpd30= 1 and is_closed = False then payment_request_dollar else 0 end) over (partition by partner_id_1 order by snap_dt) as total_dq30_plus
, sum(case when dpd60= 1 and is_closed = False  then payment_request_dollar else 0 end) over (partition by partner_id_1 order by snap_dt) as total_dq60_plus
, sum(case when dpd90= 1 and is_closed = False  then payment_request_dollar else 0 end) over (partition by partner_id_1 order by snap_dt) as total_dq90_plus
from willapay-data-staging.dev_vs_workspace.partner_daily_1
where is_agency_invoice <>1
--and date(created_at) between start_date and end_date 
order by 1,2);


select snap_dt, partner_id, last_value(un_closed_pr ignore nulls) OVER (win) AS un_closed_pr_pt, un_closed_pr
from (
select snap_dt_1 as snap_dt, partner_id_2 as partner_id
--, max(amt_requrested) as amt_requrested
, max(un_closed_pr) as un_closed_pr
, max(total_amt_requestd) as total_amt_requestd
, max(total_amt_closed) as total_amt_closed
, max(total_amt_paid) as total_amt_paid
, max(total_dq30_plus) as total_dq30_plus
, max(total_dq60_plus) as total_dq60_plus

from (
select a.snap_dt as snap_dt_1,a.partner_id as partner_id_2, b.*
from willapay-data-staging.dev_vs_workspace.partner_daily a
left outer join willapay-data-staging.dev_vs_workspace.partner_daily_2 b
on a.partner_id = b.partner_id_1
and a.snap_dt = b.snap_dt
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
group by 1,2
order by 2,1)
WINDOW win AS (partition by partner_id ORDER BY snap_dt  ROWS BETWEEN  unbounded preceding and CURRENT ROW )
order by 2,1


create or replace table willapay-data-staging.dev_vs_workspace.partner_daily_1 as (
select a.partner_id as partner_id_1, a.snap_dt, b.*
from willapay-data-staging.dev_vs_workspace.partner_daily a
left join willapay-data-staging.dev_vs_workspace.partner_checks b
on a.partner_id = b.partner_id
and a.snap_dt =  date_sub(date(b.created_at), INTERVAL 65 DAY));

select * from willapay-data-staging.dev_vs_workspace.partner_daily_1
--where created_at is not null
where partner_id_1 = 2243
and snap_dt >'2021-12-01'
order by 1,2



select * from willapay-data-staging.dev_vs_workspace.partner_checks














DECLARE start_date DATE DEFAULT DATE('2022-01-01') ;
DECLARE end_date DATE DEFAULT DATE('2022-01-01');

set start_date = DATE('2022-05-01');
set end_date = DATE('2022-05-31');

create or replace table willapay-data-staging.dev_vs_workspace.policy_simulation_pr_driver as (
  select * from `willapay-data-production.marts.dim_payment_requests`
    where transaction_id not in (144890, 594, 805, 52996, 200161, 200489, 491, 783, 143445)
    and dbt_valid_to is null
    and DATE(created_at)  between start_date and end_date
);

select approval_policy
from willapay-data-staging.dev_vs_workspace.recent_trans_dq_flags
where created_at > '2022-04-01'
group by 1

select date(a.created_at) as created_at,a.approval_policy, b.is_manually_reviewed,b.payment_request_approval_policy, count(distinct payment_request_id) as pr_count, sum(amount_requested_cent/100) as total_requested
 from 
 (select * from willapay-data-staging.dev_vs_workspace.recent_trans_dq_flags
 --from `willapay-data-production.marts.dim_payment_requests`
 where created_at between '2022-06-01' and '2022-06-06'
 and is_closed = False
 and is_agency_invoice <>1) a
 left join `willapay-data-production.marts.dim_partners` b
 on a.partner_id = b.partner_id
group by 1,2,3,4
order by 1,2,3,4


select date(a.created_at) as created_at
,a.approval_policy
, b.is_manually_reviewed
,b.payment_request_approval_policy
, sum(total_pr ) as pr_count
from 
 (select date(created_at), partner_id, approval_policy from  `willapay-data-production.marts.dim_payment_requests`
 where created_at between '2022-06-01' and '2022-06-06'
 and is_closed = False
 group by 1,2,3
 order by 1,2,3
) a
 left join `willapay-data-production.marts.dim_partners` b
 on a.partner_id = b.partner_id
group by 1,2,3,4
order by 1,2,3,4



select * from `willapay-data-production.marts.dim_partners`
is_manually_reviewed



create or replace table willapay-data-staging.dev_vs_workspace.is_existing_partner as (
  select partner_id, 1 as existing_partner_flag
  , min(dbt_valid_from) as dbt_valid_from_min
  , min(coalesce(DATE(dbt_valid_to), start_date)) as dbt_valid_to 
  from `willapay-data-production.marts.dim_partners`
  where DATE(dbt_valid_from) < start_date
  and coalesce(DATE(dbt_valid_to)) >= start_date
  group by 1,2
);

create or replace table willapay-data-staging.dev_vs_workspace.pr_check_partner_hardcut_rules as (
select   a.*
--, case when b.existing_partner_flag = 1 then 1 else 0 end as existing_partner_flag
, c.decision as partner_hard_cut_decision
/*,d.thirty_day_late_dummy_due_date
, d.sixty_day_late_dummy_due_date
, d.ninety_day_late_dummy_due_date
*/, d.is_agency_invoice
, e.model_Decision
, e.model_probability
--, d.status
--, d.is_closed
from willapay-data-staging.dev_vs_workspace.policy_simulation_pr_driver  a
left join willapay-data-staging.dev_vs_workspace.is_existing_partner  b
on a.partner_id = b.partner_id
left join `willapay-data-staging.dev_df_reporting.hardcuts_partner_asof_2022-05-01` c
on a.partner_id = c.partner_id
left join willapay-data-staging.dev_vs_workspace.recent_trans_dq_flags d
on a.payment_request_id = d.payment_request_id
left join (Select payment_request_id
            , model_probability
            , case when model_probability between 0 and 0.5 then 'approve'
                   when model_probability between 0.5 and 1 then 'manual review'
                   else null end as model_Decision
            --, ntile(5) over (order by model_probability) as risk_Decile
            from willapay-data-staging.dev_eg_adhocs.invoice_model_scores ) e
            on a.payment_request_id = e.payment_request_id);


select * from willapay-data-staging.dev_vs_workspace.pr_check_partner_hardcut_rules 
where model_Decision is null
and is_closed = False
and is_agency_invoice <>1

select partner_hard_cut_decision 
, model_Decision
, count(distinct payment_request_id) as total_PR
, count(distinct case when is_closed = False then payment_request_id else null end) as total_PR_not_closed
, sum(amount_requested_cent/100) as total_requested
, sum(case when is_closed = False then amount_requested_cent/100 else 0 end) as total_not_closed
from willapay-data-staging.dev_vs_workspace.pr_check_partner_hardcut_rules
where is_agency_invoice<>1
group by 1,2


select 
 case when risk_Decile < 3 and amount_requested_cent/100 <= 25000 then 'Model Approve' 
            when risk_Decile < 3 and amount_requested_cent/100  > 25000 then 'Model Review' 
            when risk_Decile <6 then  'Model Review' else null  end as model_decision
, partner_hard_cut_decision
, concat(FORMAT_DATETIME('%Y',created_at), '_',FORMAT_DATETIME('%B',created_at)) as Year
, count(distinct payment_request_Id) as volume_cnt
, sum(amount_requested_cent/100) as volume_rqst
, sum(thirty_day_late_dummy_due_date)/count(distinct payment_request_id) as dq30_count_rate
, sum(case when thirty_day_late_dummy_due_date = 1 then amount_requested_cent/100 else 0 end) as dq30_dollar_balance
, sum(case when thirty_day_late_dummy_due_date = 1 then amount_requested_cent/100 else 0 end)/sum(amount_requested_cent/100) as dq30_dollar_rate
, sum(sixty_day_late_dummy_due_date)/count(distinct payment_request_id) as dq60_count_rate
, sum(case when sixty_day_late_dummy_due_date = 1 then amount_requested_cent/100 else 0 end) as dq60_dollar_balance
, sum(case when sixty_day_late_dummy_due_date = 1 then amount_requested_cent/100 else 0 end)/sum(amount_requested_cent/100) as dq60_dollar_rate
from willapay-data-staging.dev_vs_workspace.pr_check_partner_hardcut_rules
where created_at > '2021-10-01'
and is_closed = False
and is_agency_invoice <>1
group by 1,2,3





select partner_hard_cut_decision
, case when risk_Decile < 3 and amount_requested_cent/100 <= 25000 then 'Model Approve' 
            when risk_Decile < 3 and amount_requested_cent/100  > 25000 then 'Model Review' 
            when risk_Decile <6 then  'Model Review' else null  end as model_decision
, is_closed
/*, case when model_decision = 'Model Approve' and partner_hard_cut_decision = 'Pass' then 'Pass'
       when model_decision = 'Model Approve' and partner_hard_cut_decision = 'Manual Reivew' then 'Pass_1'
       when model_decision = 'Auto Decline' then 'Auto Decline'*/
, count(distinct payment_request_id) as volume_count
, sum(amount_requested_cent/100) as volume_dollar
, sum(thirty_day_late_dummy_due_date) as DQ30_count
, sum(thirty_day_late_dummy_due_date)/count(distinct payment_request_id) as dq30_count_rate
, sum(case when thirty_day_late_dummy_due_date = 1 then amount_requested_cent/100 else 0 end) as dq30_dollar
, sum(case when thirty_day_late_dummy_due_date = 1 then amount_requested_cent/100 else 0 end)/sum(amount_requested_cent/100) as dq30_bal_rate
, sum(sixty_day_late_dummy_due_date) as DQ60_count
, sum(sixty_day_late_dummy_due_date)/count(distinct payment_request_id) as dq60_count_rate
, sum(case when sixty_day_late_dummy_due_date = 1 then amount_requested_cent/100 else 0 end) as dq60_dollar
, sum(case when sixty_day_late_dummy_due_date = 1 then amount_requested_cent/100 else 0 end)/sum(amount_requested_cent/100) as dq60_bal_rate
from willapay-data-staging.dev_vs_workspace.pr_check_partner_hardcut_rules
where is_agency_invoice <>1
--and status not in ( 'refunded', 'closed', 'closed_by_admin', 'closed_by_partner','rejected') 
--and is_closed is FALSE
group by 1,2,3

select * from willapay-data-staging.dev_vs_workspace.pr_check_partner_hardcut_rules
where is_closed = True
--and partner_hard_cut_decision not in ('Auto Decline'). --- not in auto but closed are mostly due to duplicates, not a business, not supported, or closed by partner 
--and partner_hard_cut_decision in ('Pass') --- caught by limits mostly 
and partner_hard_cut_decision in ('Manual Review')
and substatus = 'closed_by_admin'

select * from `willapay-data-staging.dev_df_reporting.partner_hardcuts_asof_2022-01-01`
where partner_id = 132582
--132120 partner is manually_reviewed and is auto decline because of overdraft 
-- 134750

select partner_id, count(distinct payment_request_id)
, sum(amount_requested_cent/100) 
 from willapay-data-staging.dev_vs_workspace.pr_check_partner_hardcut_rules
where partner_hard_cut_decision = 'Auto Decline'
and is_closed = False
group by 1
order by 2 desc
order by amount_requested_cent desc
group by 1 --- out of 400 297 was closed 




---

select partner_has_history, count(distinct partner_id)
from `willapay-data-staging.dev_vs_workspace.payment_hardcut_Checks`
group by 1

create or replace table `willapay-data-staging.dev_vs_workspace.payment_hardcut_Checks` as (
select * 
, case when partner_suspended_check = 'Fail' then 'Auto Decline'
       --when partner_exception_check = 'Pass' then 'Auto Approve'
       when current_dpc120_rate_check = 'Fail' then 'Auto Decline' -- Open balance for over 120 days 
       when current_dpc90_rate_check = 'Fail' then 'Auto Decline' -- Open balance for over 90 days
       when partner_overdraft_check = 'Fail' then 'Manual Review'
       --when manually_reviewed_partner = 'always_manual_review' then 'Manual Review'
       --when manually_reviewed_partner = 'always_automatic_approval' then 'Auto Approve'
       when partner_has_history = 'Fail' then 'Manul Review' else 'Pass' end as hardcut_decision 
from (
select partner_id
,pr_created_at as asof_date
, case when partner_has_history = 0 then 'Fail' else 'Pass' end as partner_has_history 
, case when dpc90_rate > 0.15 then 'Fail' else 'Pass' end as current_dpc90_rate_check
,  case when dpc120_rate > 0.05 then 'Fail' else 'Pass' end as current_dpc120_rate_check
, case when suspension_state = 'partner_suspended' then 'Fail' else 'Pass' end as partner_suspended_check
, case when open_balance_usd > 50 then 'Fail' else 'Pass' end as partner_overdraft_check
--, case when partner_exception = 0 then 'Fail' else 'Pass' end as partner_exception_check
from (
select partner_id
, date(pr_created_at) as pr_created_at
, payment_request_id
, num_pre_p 
, closed_num_pre_p
, num_pre_p -closed_num_pre_p as num_pre_approved_p 
, amount_requested_usd
, manually_reviewed_partner
, suspension_state
, open_balance_usd
, case when (sum(case when (num_pre_p -closed_num_pre_p) > 2 and date(pr_created_at) < date_sub(current_date, interval 60 day) then 1 else 0 end) over (partition by partner_id order by pr_created_at)) > 0 then 1 else 0 end as partner_has_history
, sum(case when ifnull(flag_30p_dpd,0) = 1 and status<>'closed' then amount_requested_usd else 0 end) over (partition by partner_id order by pr_created_at)
/ (nullif((sum(case when status <> 'closed' then amount_requested_usd else 0 end) over (partition by partner_id order by pr_created_at)),0))as dpd30_rate
, sum(amount_requested_usd) over (partition by partner_id order by pr_created_at) as total_requested
, (sum(case when ifnull(flag_90p_dpc,0) =1 and status <> 'closed' and status <> 'paid' then amount_requested_usd else 0 end) over (partition by partner_id order by pr_created_at)) as dpc90_amount
, (sum(case when ifnull(flag_120p_dpc,0) =1 and status <> 'closed' and status <> 'paid' then amount_requested_usd else 0 end) over (partition by partner_id order by pr_created_at)) as dpc120_amount
, (sum(case when ifnull(flag_90p_dpc,0) =1 and status <> 'closed' and status <> 'paid' then amount_requested_usd else 0 end) over (partition by partner_id order by pr_created_at))
/(nullif((sum(case when status <> 'closed' then amount_requested_usd else 0 end) over (partition by partner_id order by pr_created_at)),0))as dpc90_rate
, ifnull(flag_90p_dpc,0) as flag_90p_dpc
, ifnull(flag_120p_dpc,0) as flag_120p_dpc
, (sum(case when flag_120p_dpc =1 and status <> 'closed' then amount_requested_usd else 0 end) over (partition by partner_id order by pr_created_at))
/(nullif((sum(case when status <> 'closed' then amount_requested_usd else 0 end) over (partition by partner_id order by pr_created_at)),0))as dpc120_rate
  from `willapay-data-staging.dev_eg_reporting.payment_request_features`
/*where  partner_id = 132186
order by pr_created_at*/
--where 
where is_agency_partner= False
--and date(pr_created_at) < date_sub(current_date, interval 15 day)
)

QUALIFY ROW_NUMBER() OVER (PARTITION BY partner_id ORDER BY pr_created_at DESC) = 1
)) ;

, case when open_balance_usd > 50 then 'Fail' else 'Pass' end as partner_overdraft_check


select hardcut_decision,partner_suspended_check, current_dpc90_rate_check,current_dpc120_rate_check,partner_has_history,partner_overdraft_check  
, count(distinct case when substatus <> 'closed_by_partner' then payment_request_id else null end) as total_pr
, sum(case when substatus <> 'closed_by_partner' then amount_requested_cent/100 else 0 end) as total_pr_dollar
from (
select a.*, b.*
from (select *
    from `willapay-data-production.marts.dim_payment_requests`
    where transaction_id not in (144890, 594, 805, 52996, 200161, 200489, 491, 783, 143445)
    and dbt_valid_to is null
   and DATE(created_at)  >= '2022-06-01' ) a
   left join `willapay-data-staging.dev_vs_workspace.payment_hardcut_Checks`  b
   on a.partner_id = b.partner_id)
   group by 1,2,3,4,5,6
   order by total_pr desc


select partner_id,pr_created_at,invoices_amount_usd, amount_requested_usd , open_balance_usd, flag_120p_dpc 
from `willapay-data-staging.dev_eg_reporting.payment_request_features`
--where open_balance >0
--and suspension_state <>'partner_suspended'
where  partner_id = 132186
order by pr_created_at


select * from  `willapay-data-staging.dev_eg_reporting.payment_request_features`
where date(pr_created_at) < date_sub(date(current_date()), interval 65 day) 
and partner_id = 132186
--and manually_reviewed_partner = True
--and suspension_state<> 'partner_suspended'
order by pr_created_at

There are flags dpd 

num_pre_p
num_pre_c
paid_within_30d_after_due_date_amount_usd
manually_reviewed_partner
is_suspended

paid_within_30d_after_due_date_amount_rate





