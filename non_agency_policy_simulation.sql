
----1) Driver table 
create or replace table willapay-data-staging.dev_vs_workspace.policy_simulation_driver as (
  select * 

   , (case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(current_date,due_date,day))) >= 30 then 1 else 0 end) as dpd30
    , (case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(current_date,due_date,day))) >= 60 then 1 else 0 end) as dpd60
      , (case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(current_date,due_date,day))) >= 90 then 1 else 0 end) as dpd90
, (case when (date_diff(current_date,date(created_at),day)) >= 60 and is_paid = False then 1 else 0 end) as dpc60
, (case when (date_diff(current_date,date(created_at),day)) >= 90 and is_paid = False then 1 else 0 end) as dpc90
, (case when (date_diff(current_date,date(created_at),day)) >= 120 and is_paid = False then 1 else 0 end) as dpc120
    
    
  from willapay-data-production.marts.dim_payment_requests
  where created_at between '2022-01-01' and '2022-04-15'
  and dbt_valid_to is null
  and is_closed = False
  and   case when split(payer_email_address, '@')[offset(1)] in (
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
        then 1 else 0 end <>1
  );

  /*select dpc120, sum(amount_requested_cent/100)
  from willapay-data-staging.dev_vs_workspace.policy_simulation_driver
  group by 1
*/
--- Partner checks - get approval policy and suspension state 

/*
select count(distinct payment_request_id_incoming)
from willapay-data-staging.dev_vs_workspace.policy_simulation_partner_hardcut_checks_1 ---554

select count(distinct payment_request_id_incoming)
from willapay-data-staging.dev_vs_workspace.policy_simulation_partner_hardcuts_checks --- 586

select count(distinct payment_request_id_incoming)
from willapay-data-staging.dev_vs_workspace.policy_simulation_client_hardcuts_checks --- 568

select count(distinct payment_request_id) from willapay-data-staging.dev_vs_workspace.policy_simulation_driver

*/

create or replace table willapay-data-staging.dev_vs_workspace.policy_simulation_partner_hardcut_checks_1 as (
select a.payment_request_id as payment_request_id_incoming
, a.created_at as created_at_incoming
,a.amount_requested_cent/100 as amount_requested_incoming
, a.partner_id
, a.dpd60
, a.dpd90
, dpc90
, dpc120
, b.is_suspended
, b.payment_request_approval_policy
, b.dbt_valid_from
, b.dbt_valid_to
from (select * from willapay-data-staging.dev_vs_workspace.policy_simulation_driver ---where partner_id = 63789
) a
left join willapay-data-production.marts.dim_partners b
on a.partner_id = b.partner_id
and date(b.dbt_valid_from) < date(a.created_at)
and date(b.created_at) < date(a.created_at)
qualify row_number() over (partition  by b.partner_id order by coalesce(date(b.dbt_valid_to),date(a.created_at)) desc) = 1
order by 3
);


--- partner hardcut and limits which is a look at the health of current outstanding balance for partner 
create or replace table willapay-data-staging.dev_vs_workspace.policy_simulation_partner_hardcuts_checks as (
select *
, case when dpd60_count>=2 and dpd60_plus_rate >=0.20 then 0 
       when dpd30_count>=2 and dpd30_plus_rate >=0.25 then 0
       when dpd0_count>=3 and dpd0_plus_rate >=0.90 then 0
    else 50000 end as limit_with_history
from (
select
payment_request_id_incoming
, created_at_incoming
, amount_requested_incoming
, partner_id
, sum(amount_requested_cent/100) as total_requested
, sum(case when is_closed = False and is_approved = True then amount_requested_cent/100 else null end) as total_approved
, sum(case when is_closed = False and is_approved = True and is_paid = False then amount_requested_cent/100 else null end) as total_outstanding

, sum(case when is_closed = False and is_approved = True and is_paid = False and date_diff(date(created_at_incoming), date(created_at), day) > 90 then amount_requested_cent/100 else null end) as dpc90

, sum(case when is_closed = False and is_approved = True and is_paid = False and date_diff(date(created_at_incoming), date(created_at), day) > 120 then amount_requested_cent/100 else null end) as dpc120

, sum(case when is_closed = False and is_approved = True and is_paid = False and date_diff(date(created_at_incoming), date(created_at), day) > 90 then amount_requested_cent/100 else null end)/ sum(case when is_closed = False and is_approved = True and is_paid = False then amount_requested_cent/100 else null end) as dpc90_rate --- this is the ratio of dpc90+ relative to you total outstanding 

, sum(case when is_closed = False and is_approved = True and is_paid = False and date_diff(date(created_at_incoming), date(created_at), day) > 120 then amount_requested_cent/100 else null end)/ sum(case when is_closed = False and is_approved = True and is_paid = False then amount_requested_cent/100 else null end) as dpc120_rate --- this is the ratio of dpc90+ relative to you total outstanding 

, sum((case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(date(created_at_incoming),due_date,day))) >= 0 and is_closed =False and is_approved = True and is_paid = False then 1 else 0 end)) as dpd0_count
  
  , sum((case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(date(created_at_incoming),due_date,day))) >= 30 and is_closed =False and is_approved = True and is_paid = False  then 1 else 0 end)) as dpd30_count

 , sum((case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(date(created_at_incoming),due_date,day))) >= 60  and is_closed =False and is_approved = True and is_paid = False then 1 else 0 end)) as dpd60_count

, sum((case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(date(created_at_incoming),due_date,day))) >= 0 and is_closed =False and is_approved = True and is_paid = False  then amount_requested_cent/100 else 0 end)) /sum(case when is_closed = False and is_approved = True and is_paid = False then amount_requested_cent/100 else null end) as dpd0_plus_rate

, sum((case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(date(created_at_incoming),due_date,day))) >= 30 and is_closed =False and is_approved = True and is_paid = False  then amount_requested_cent/100 else 0 end)) /sum(case when is_closed = False and is_approved = True and is_paid = False then amount_requested_cent/100 else null end) as dpd30_plus_rate

, sum((case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(date(created_at_incoming),due_date,day))) >= 60 and is_closed =False and is_approved = True and is_paid = False  then amount_requested_cent/100 else 0 end)) /sum(case when is_closed = False and is_approved = True and is_paid = False then amount_requested_cent/100 else null end) as dpd60_plus_rate

from (
select a.payment_request_id as payment_request_id_incoming
, a.created_at as created_at_incoming
, a.amount_requested_cent/100 as amount_requested_incoming
, b.*
from (select * from willapay-data-staging.dev_vs_workspace.policy_simulation_driver ---where partner_id = 63789
) a
left join willapay-data-production.marts.dim_payment_requests b
on a.partner_id = b.partner_id
and date(b.dbt_valid_from) < date(a.created_at)
and date(b.created_at) < date(a.created_at)
qualify row_number() over (partition  by b.payment_request_id order by coalesce(date(b.dbt_valid_to),date(a.created_at)) desc) = 1
order by 3)
group by 1,2,3,4));



--- client hardcut and limits which is a look at the health of current outstanding balance for client 
create or replace table willapay-data-staging.dev_vs_workspace.policy_simulation_client_hardcuts_checks as (
select * 
,  case when dpd60_count>=2 and dpd60_plus_rate >=0.15 then 0 
       when dpd30_count>=2 and dpd30_plus_rate >=0.20 then 0
       when dpd0_count>=3 and dpd0_plus_rate >=0.75 then 0
    else 50000 end as limit_with_history

  from (
select payment_request_id_incoming
, created_at_incoming
, amount_requested_incoming
, invoiced_customer_id
, sum(amount_requested_cent/100) as total_requested
, sum(case when is_closed = False and is_approved = True then amount_requested_cent/100 else null end) as total_approved
, sum(case when is_closed = False and is_approved = True and is_paid = False then amount_requested_cent/100 else null end) as total_outstanding

, sum(case when is_closed = False and is_approved = True and is_paid = False and date_diff(date(created_at_incoming), date(created_at), day) > 90 then amount_requested_cent/100 else null end) as dpc90

, sum(case when is_closed = False and is_approved = True and is_paid = False and date_diff(date(created_at_incoming), date(created_at), day) > 120 then amount_requested_cent/100 else null end) as dpc120

, sum(case when is_closed = False and is_approved = True and is_paid = False and date_diff(date(created_at_incoming), date(created_at), day) > 90 then amount_requested_cent/100 else null end)/ sum(case when is_closed = False and is_approved = True and is_paid = False then amount_requested_cent/100 else null end) as dpc90_rate --- this is the ratio of dpc90+ relative to you total outstanding 

, sum(case when is_closed = False and is_approved = True and is_paid = False and date_diff(date(created_at_incoming), date(created_at), day) > 120 then amount_requested_cent/100 else null end)/ sum(case when is_closed = False and is_approved = True and is_paid = False then amount_requested_cent/100 else null end) as dpc120_rate --- this is the ratio of dpc90+ relative to you total outstanding 

, sum((case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(date(created_at_incoming),due_date,day))) >= 0 and is_closed =False and is_approved = True and is_paid = False then 1 else 0 end)) as dpd0_count
  
  , sum((case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(date(created_at_incoming),due_date,day))) >= 30 and is_closed =False and is_approved = True and is_paid = False  then 1 else 0 end)) as dpd30_count

 , sum((case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(date(created_at_incoming),due_date,day))) >= 60  and is_closed =False and is_approved = True and is_paid = False then 1 else 0 end)) as dpd60_count

, sum((case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(date(created_at_incoming),due_date,day))) >= 0 and is_closed =False and is_approved = True and is_paid = False  then amount_requested_cent/100 else 0 end)) /sum(case when is_closed = False and is_approved = True and is_paid = False then amount_requested_cent/100 else null end) as dpd0_plus_rate

, sum((case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(date(created_at_incoming),due_date,day))) >= 30 and is_closed =False and is_approved = True and is_paid = False  then amount_requested_cent/100 else 0 end)) /sum(case when is_closed = False and is_approved = True and is_paid = False then amount_requested_cent/100 else null end) as dpd30_plus_rate

, sum((case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(date(created_at_incoming),due_date,day))) >= 60 and is_closed =False and is_approved = True and is_paid = False  then amount_requested_cent/100 else 0 end)) /sum(case when is_closed = False and is_approved = True and is_paid = False then amount_requested_cent/100 else null end) as dpd60_plus_rate

from (
select a.payment_request_id as payment_request_id_incoming
, a.created_at as created_at_incoming
,a.amount_requested_cent/100 as amount_requested_incoming
, b.*
from (select * from willapay-data-staging.dev_vs_workspace.policy_simulation_driver ---where partner_id = 63789
) a
left join willapay-data-production.marts.dim_payment_requests b
on a.invoiced_customer_id = b.invoiced_customer_id
and date(b.dbt_valid_from) < date(a.created_at)
and date(b.created_at) < date(a.created_at)
qualify row_number() over (partition  by b.payment_request_id order by coalesce(date(b.dbt_valid_to),date(a.created_at)) desc) = 1
order by 3)
group by 1,2,3,4));

--- Has history 
---- Patner segmentation 
create or replace table willapay-data-staging.dev_vs_workspace.policy_simulation_partner_segmentation as ( 
select payment_request_id_incoming
, created_at_incoming
, amount_requested_incoming
, partner_id
, sum(amount_requested_cent/100) as total_requested
, sum(case when is_closed = False and is_approved = True then amount_requested_cent/100 else null end) as total_approved
, count(case when is_closed = False and is_approved = True then payment_request_id else null end) as total_approved_count

, sum((case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(date(created_at_incoming),due_date,day))) >= 30 and is_closed =False and is_approved = True then amount_requested_cent/100 else 0 end)) /sum(case when is_closed = False and is_approved = True  then amount_requested_cent/100 else null end) as dpd30_plus_rate


from (
select a.payment_request_id as payment_request_id_incoming
, a.created_at as created_at_incoming
,a.amount_requested_cent/100 as amount_requested_incoming
, b.*
--, row_number() over (partition by partner_id, order by created_at)
from (select * from willapay-data-staging.dev_vs_workspace.policy_simulation_driver -- where partner_id = 63789
) a
left join willapay-data-production.marts.dim_payment_requests b
on a.partner_id = b.partner_id
and date(b.dbt_valid_from) < date_sub(date(a.created_at),interval 65 day) --- this 60 day is to be redone in early July. This will evolve into 65 for net30, 95 for net 60 and 125 for net 90. We will also reward newer partners that pay really fast  
and date(b.created_at) < date_sub(date(a.created_at),interval 65 day)
qualify row_number() over (partition  by b.payment_request_id order by coalesce(date(b.dbt_valid_to),date(a.created_at)) desc) = 1
order by 3)
group by 1,2,3,4);


---- Client segmentation
create or replace table willapay-data-staging.dev_vs_workspace.policy_simulation_client_segmentation as ( 
select payment_request_id_incoming
, created_at_incoming
, amount_requested_incoming
, invoiced_customer_id
, sum(amount_requested_cent/100) as total_requested
, sum(case when is_closed = False and is_approved = True then amount_requested_cent/100 else null end) as total_approved
, count(case when is_closed = False and is_approved = True then payment_request_id else null end) as total_approved_count
, sum((case when ifnull(date_diff(cast(paid_at as date), due_date, day),(date_diff(date(created_at_incoming),due_date,day))) >= 30 and is_closed =False and is_approved = True then amount_requested_cent/100 else 0 end)) /sum(case when is_closed = False and is_approved = True  then amount_requested_cent/100 else null end) as dpd30_plus_rate
from (
select a.payment_request_id as payment_request_id_incoming
, a.created_at as created_at_incoming
,a.amount_requested_cent/100 as amount_requested_incoming
, b.*
--, row_number() over (partition by partner_id, order by created_at)
from (select * from willapay-data-staging.dev_vs_workspace.policy_simulation_driver ---where partner_id = 63789
) a
left join willapay-data-production.marts.dim_payment_requests b
on a.invoiced_customer_id = b.invoiced_customer_id
and date(b.dbt_valid_from) < date_sub(date(a.created_at),interval 60 day)
and date(b.created_at) < date_sub(date(a.created_at),interval 60 day)
qualify row_number() over (partition  by b.payment_request_id order by coalesce(date(b.dbt_valid_to),date(a.created_at)) desc) = 1
order by 3)
group by 1,2,3,4);



----lets put it all together 
create or replace table willapay-data-staging.dev_vs_workspace.policy_simulation_all_rules as (
select 
driver.payment_request_id as payment_request_id_incoming
, driver.created_at as created_at_incoming
, driver.due_date
, driver.partner_id
, driver.amount_requested_cent/100 as amount_requested_incoming
, driver.payer_email_address as payer_email_address_incoming
, a.is_suspended
, a.payment_request_approval_policy
, driver.dpd30
, driver.dpd60
, driver.dpd90
, driver.dpc60
, driver.dpc90
, driver.dpc120
, b.total_requested as total_requested_partner
, b.total_approved as total_approved_partner
, b.total_outstanding as total_outstanding_partner
, b.dpc90_rate as dpc90_rate_partner
, b.dpc120_rate as dpc120_rate_partner
, b.limit_with_history as limit_with_history_partner
, c.total_requested as total_requested_client
, c.total_approved as total_approved_client
, c.total_outstanding as total_outstanding_client
, c.dpc90_rate as dpc90_rate_client
, c.dpc120_rate as dpc120_rate_client
, c.limit_with_history as limit_with_history_client
, case when ifnull(d.total_approved,0) > 2 then True else False end as parnter_has_history
, case when ifnull(e.total_approved,0) > 2 then True else False end as client_has_history
, case when ifnull(d.total_approved,0) <= 2 then 'No History'
       when ifnull(d.total_approved,0) > 2 and ifnull(d.dpd30_plus_rate, 0) < 0.15 then 'Bucket 1'
       when ifnull(d.total_approved,0) > 2 and ifnull(d.dpd30_plus_rate, 0) < 0.30 then 'Bucket 2'
       when ifnull(d.total_approved,0) > 2 and ifnull(d.dpd30_plus_rate, 0) < 1.01 then 'Bucket 3'
    else null end as partner_segmentation

, case when ifnull(e.total_approved,0) <= 2 then 'No History'
       when ifnull(e.total_approved,0) > 2 and ifnull(e.dpd30_plus_rate, 0) < 0.15 then 'Bucket 1'
       when ifnull(e.total_approved,0) > 2 and ifnull(e.dpd30_plus_rate, 0) < 0.30 then 'Bucket 2'
       when ifnull(e.total_approved,0) > 2 and ifnull(e.dpd30_plus_rate, 0) < 1.01 then 'Bucket 3'
    else null end as client_segmentation
, f.model_probability
from willapay-data-staging.dev_vs_workspace.policy_simulation_driver driver
left join  willapay-data-staging.dev_vs_workspace.policy_simulation_partner_hardcut_checks_1 a
on driver.payment_request_id = a.payment_request_id_incoming
left join  willapay-data-staging.dev_vs_workspace.policy_simulation_partner_hardcuts_checks b
on driver.payment_request_id = b.payment_request_id_incoming
left join willapay-data-staging.dev_vs_workspace.policy_simulation_client_hardcuts_checks c
on driver.payment_request_id = c.payment_request_id_incoming
left join willapay-data-staging.dev_vs_workspace.policy_simulation_partner_segmentation d
on driver.payment_request_id = d.payment_request_id_incoming
left join willapay-data-staging.dev_vs_workspace.policy_simulation_client_segmentation e
on driver.payment_request_id = e.payment_request_id_incoming
left join willapay-data-staging.dev_eg_adhocs.invoice_model_scores f
on driver.payment_request_id = f.payment_request_id);



create or replace table willapay-data-staging.dev_vs_workspace.policy_simulation_hardcut_pass as (
select *, 
case when partner_suspended_check = 'Fail' then 'partner_suspended_check'
       --when partner_exception_check = 'Pass' then 'Auto Approve'
       when current_dpc90_rate_check_client = 'Fail' then 'current_dpc90_rate_check_client' -- Open balance for over 120 days 
       when current_dpc120_rate_check_client = 'Fail' then 'current_dpc120_rate_check_client' -- Open balance for over 90 days
       when current_dpc90_rate_check_partner = 'Fail' then 'current_dpc90_rate_check_partner'
       when current_dpc120_rate_check_partner = 'Fail' then 'current_dpc120_rate_check_partner'
       when partner_always_manual_check = 'Fail' then 'partner_always_manual_check'
       --when model_probability > 0.5 then 'Model_cut_Fail'
       else 'Pass' end as hardcut_check
       --when manually_reviewed_partner = 'always_automatic_approval' then 'Auto Approve'
      /* , count(distinct payment_request_id_incoming) as total_pr
       , sum(amount_requested_incoming) as total_requested
       , sum(case when dpd60= 1 then amount_requested_incoming else 0 end) as total_dpd60_amt
       , sum(case when dpd90= 1 then amount_requested_incoming else 0 end) as total_dpd90_amt
       , sum(case when dpd60= 1 then amount_requested_incoming else 0 end)/sum(amount_requested_incoming) as dpd_60_rate
       , sum(case when dpd90= 1 then amount_requested_incoming else 0 end)/sum(amount_requested_incoming) as dpd_90_rate
       , sum(case when dpd90_unpaid= 1 then amount_requested_incoming else 0 end)/sum(amount_requested_incoming) as dpd_90_unpaid_rate
       , sum(case when dpc120= 1 then amount_requested_incoming else 0 end) as dpc_12_unpaid
       , sum(case when dpc120= 1 then amount_requested_incoming else 0 end)/sum(amount_requested_incoming) as dpc_120_unpaid_rate
*/
from (
select 
payment_request_id_incoming
, amount_requested_incoming
, payer_email_address_incoming
, dpd90
, dpd60
, dpd30
, dpc60
, dpc90
, dpc120
, due_date
, model_probability
, case when dpc90_rate_client > 0.15 then 'Fail' else 'Pass' end as current_dpc90_rate_check_client
,  case when dpc120_rate_client > 0.05 then 'Fail' else 'Pass' end as current_dpc120_rate_check_client
, case when dpc90_rate_partner > 0.15 then 'Fail' else 'Pass' end as current_dpc90_rate_check_partner
,  case when dpc120_rate_partner > 0.05 then 'Fail' else 'Pass' end as current_dpc120_rate_check_partner
, case when payment_request_approval_policy = 'always_manual_review' then 'Fail' else 'Pass' end as  partner_always_manual_check
, case when is_suspended = True then 'Fail' else 'Pass' end as partner_suspended_check
--, case when open_balance_usd > 50 then 'Fail' else 'Pass' end as partner_overdraft_check
, case when parnter_has_history= False then 'Fail' else 'Pass' end as parnter_has_history_check
, case when client_has_history = False then 'Fail' else 'Pass' end as client_has_history_check
, client_has_history
, parnter_has_history
, partner_segmentation
, client_segmentation 
from willapay-data-staging.dev_vs_workspace.policy_simulation_all_rules
));



create or replace table willapay-data-staging.dev_vs_workspace.policy_simulation_final_decision as (
select * 
, case when hardcut_check = 'Pass' and partner_segmentation = 'Bucket 1' and client_segmentation in ('Bucket 1', 'Bucket 2') then 'Auto Approve'
       when hardcut_check = 'Pass' and client_segmentation = 'Bucket 1' and partner_segmentation in ('Bucket 1', 'Bucket 2', 'No History') then 'Auto Approve'
       when hardcut_check = 'Pass' and partner_segmentation in ('Bucket 1') and client_segmentation in ('No History') and case when lower(split(payer_email_address_incoming, '@')[offset(1)]) in ("gmail.com",
        "yahoo.com",
        "icloud.com",
        "aol.com"
        ) 
        then 1 else 0 end <> 1 then 'Auto Approve'
       else 'Manual review' end as auto_decision
from willapay-data-staging.dev_vs_workspace.policy_simulation_hardcut_pass
);


select auto_decision,dpd30, sum(payment_request_id_incoming/100), count(payment_request_id_incoming)
  from willapay-data-staging.dev_vs_workspace.policy_simulation_final_decision
  where payment_request_id_incoming not in  (202361,202542)
  group by 1,2;

select auto_decision
,sum(case when dpd30=1 then payment_request_id_incoming/100 else 0 end)/ sum(payment_request_id_incoming/100) as dpd30_rate
,sum(case when dpd60=1 then payment_request_id_incoming/100 else 0 end)/ sum(payment_request_id_incoming/100) as dpd60_rate
, sum(payment_request_id_incoming/100) as total_volume
, count(payment_request_id_incoming)
  from willapay-data-staging.dev_vs_workspace.policy_simulation_final_decision
  where payment_request_id_incoming not in  (202361,202542)
  and due_date <=date_sub(current_date, interval 60 day)
  group by 1



/*
create or replace table willapay-data-staging.dev_vs_workspace.policy_simulation_final_decision_june_2022 as (
  select * from willapay-data-staging.dev_vs_workspace.policy_simulation_final_decision
);


create or replace table willapay-data-staging.dev_vs_workspace.policy_simulation_final_decision_jan_feb as (
  select * from willapay-data-staging.dev_vs_workspace.policy_simulation_final_decision
);
*/













