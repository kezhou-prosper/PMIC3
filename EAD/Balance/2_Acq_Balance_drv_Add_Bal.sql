CREATE OR REPLACE TABLE Group_Risk.npc_acq_balance_model_step2_balance AS
SELECT
  drv.*, 
  IFNULL(s0.ifo_current_balance, 0)  AS BalanceT0,
  IFNULL(s1.ifo_current_balance, 0)  AS BalanceT1,
  IFNULL(s2.ifo_current_balance, 0)  AS BalanceT2,
  IFNULL(s3.ifo_current_balance, 0)  AS BalanceT3,
  IFNULL(s4.ifo_current_balance, 0)  AS BalanceT4,
  IFNULL(s5.ifo_current_balance, 0)  AS BalanceT5,
  IFNULL(s6.ifo_current_balance, 0)  AS BalanceT6,
  IFNULL(s7.ifo_current_balance, 0)  AS BalanceT7,
  IFNULL(s8.ifo_current_balance, 0)  AS BalanceT8,
  IFNULL(s9.ifo_current_balance, 0)  AS BalanceT9,
  IFNULL(s10.ifo_current_balance, 0) AS BalanceT10,
  IFNULL(s11.ifo_current_balance, 0) AS BalanceT11,
  IFNULL(s12.ifo_current_balance, 0) AS BalanceT12,
  IFNULL(s13.ifo_current_balance, 0) AS BalanceT13,
  IFNULL(s14.ifo_current_balance, 0) AS BalanceT14,
  IFNULL(s15.ifo_current_balance, 0) AS BalanceT15,
  IFNULL(s16.ifo_current_balance, 0) AS BalanceT16,
  IFNULL(s17.ifo_current_balance, 0) AS BalanceT17,
  IFNULL(s18.ifo_current_balance, 0) AS BalanceT18,
  IFNULL(s19.ifo_current_balance, 0) AS BalanceT19,
  IFNULL(s20.ifo_current_balance, 0) AS BalanceT20,
  IFNULL(s21.ifo_current_balance, 0) AS BalanceT21,
  IFNULL(s22.ifo_current_balance, 0) AS BalanceT22,
  IFNULL(s23.ifo_current_balance, 0) AS BalanceT23,
  IFNULL(s24.ifo_current_balance, 0) AS BalanceT24,

  IF(s0.ifo_current_balance  IS NULL, 1, 0) AS BalanceT0MissingFlag,
  IF(s1.ifo_current_balance  IS NULL, 1, 0) AS BalanceT1MissingFlag,
  IF(s2.ifo_current_balance  IS NULL, 1, 0) AS BalanceT2MissingFlag,
  IF(s3.ifo_current_balance  IS NULL, 1, 0) AS BalanceT3MissingFlag,
  IF(s4.ifo_current_balance  IS NULL, 1, 0) AS BalanceT4MissingFlag,
  IF(s5.ifo_current_balance  IS NULL, 1, 0) AS BalanceT5MissingFlag,
  IF(s6.ifo_current_balance  IS NULL, 1, 0) AS BalanceT6MissingFlag,
  IF(s7.ifo_current_balance  IS NULL, 1, 0) AS BalanceT7MissingFlag,
  IF(s8.ifo_current_balance  IS NULL, 1, 0) AS BalanceT8MissingFlag,
  IF(s9.ifo_current_balance  IS NULL, 1, 0) AS BalanceT9MissingFlag,
  IF(s10.ifo_current_balance IS NULL, 1, 0) AS BalanceT10MissingFlag,
  IF(s11.ifo_current_balance IS NULL, 1, 0) AS BalanceT11MissingFlag,
  IF(s12.ifo_current_balance IS NULL, 1, 0) AS BalanceT12MissingFlag,
  IF(s13.ifo_current_balance IS NULL, 1, 0) AS BalanceT13MissingFlag,
  IF(s14.ifo_current_balance IS NULL, 1, 0) AS BalanceT14MissingFlag,
  IF(s15.ifo_current_balance IS NULL, 1, 0) AS BalanceT15MissingFlag,
  IF(s16.ifo_current_balance IS NULL, 1, 0) AS BalanceT16MissingFlag,
  IF(s17.ifo_current_balance IS NULL, 1, 0) AS BalanceT17MissingFlag,
  IF(s18.ifo_current_balance IS NULL, 1, 0) AS BalanceT18MissingFlag,
  IF(s19.ifo_current_balance IS NULL, 1, 0) AS BalanceT19MissingFlag,
  IF(s20.ifo_current_balance IS NULL, 1, 0) AS BalanceT20MissingFlag,
  IF(s21.ifo_current_balance IS NULL, 1, 0) AS BalanceT21MissingFlag,
  IF(s22.ifo_current_balance IS NULL, 1, 0) AS BalanceT22MissingFlag,
  IF(s23.ifo_current_balance IS NULL, 1, 0) AS BalanceT23MissingFlag,
  IF(s24.ifo_current_balance IS NULL, 1, 0) AS BalanceT24MissingFlag,

  -- mdl_balance_t24: balance at the earliest credit-event MOB (COBK first;
  -- otherwise the earlier of settlement / LT-workout). Falls back to T24 when
  -- the account had no event by MOB24.
  CASE
    WHEN COALESCE(
           drv.first_cobk_mob,
           LEAST(IFNULL(drv.first_settlement_mob, 99),
                 IFNULL(drv.first_ltworkout_mob,  99))
         ) <= 24
    THEN
      CASE COALESCE(
             drv.first_cobk_mob,
             LEAST(IFNULL(drv.first_settlement_mob, 99),
                   IFNULL(drv.first_ltworkout_mob,  99))
           )
        WHEN 1  THEN IFNULL(s1.ifo_current_balance,  0)
        WHEN 2  THEN IFNULL(s2.ifo_current_balance,  0)
        WHEN 3  THEN IFNULL(s3.ifo_current_balance,  0)
        WHEN 4  THEN IFNULL(s4.ifo_current_balance,  0)
        WHEN 5  THEN IFNULL(s5.ifo_current_balance,  0)
        WHEN 6  THEN IFNULL(s6.ifo_current_balance,  0)
        WHEN 7  THEN IFNULL(s7.ifo_current_balance,  0)
        WHEN 8  THEN IFNULL(s8.ifo_current_balance,  0)
        WHEN 9  THEN IFNULL(s9.ifo_current_balance,  0)
        WHEN 10 THEN IFNULL(s10.ifo_current_balance, 0)
        WHEN 11 THEN IFNULL(s11.ifo_current_balance, 0)
        WHEN 12 THEN IFNULL(s12.ifo_current_balance, 0)
        WHEN 13 THEN IFNULL(s13.ifo_current_balance, 0)
        WHEN 14 THEN IFNULL(s14.ifo_current_balance, 0)
        WHEN 15 THEN IFNULL(s15.ifo_current_balance, 0)
        WHEN 16 THEN IFNULL(s16.ifo_current_balance, 0)
        WHEN 17 THEN IFNULL(s17.ifo_current_balance, 0)
        WHEN 18 THEN IFNULL(s18.ifo_current_balance, 0)
        WHEN 19 THEN IFNULL(s19.ifo_current_balance, 0)
        WHEN 20 THEN IFNULL(s20.ifo_current_balance, 0)
        WHEN 21 THEN IFNULL(s21.ifo_current_balance, 0)
        WHEN 22 THEN IFNULL(s22.ifo_current_balance, 0)
        WHEN 23 THEN IFNULL(s23.ifo_current_balance, 0)
        WHEN 24 THEN IFNULL(s24.ifo_current_balance, 0)
      END
    ELSE IFNULL(s24.ifo_current_balance, 0)
  END AS mdl_balance_t24

FROM Group_Risk.pmic3_2201_2403 drv

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s0
  ON RIGHT(s0.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s0.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 0 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s1
  ON RIGHT(s1.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s1.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 1 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s2
  ON RIGHT(s2.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s2.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 2 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s3
  ON RIGHT(s3.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s3.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 3 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s4
  ON RIGHT(s4.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s4.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 4 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s5
  ON RIGHT(s5.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s5.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 5 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s6
  ON RIGHT(s6.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s6.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 6 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s7
  ON RIGHT(s7.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s7.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 7 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s8
  ON RIGHT(s8.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s8.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 8 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s9
  ON RIGHT(s9.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s9.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 9 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s10
  ON RIGHT(s10.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s10.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 10 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s11
  ON RIGHT(s11.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s11.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 11 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s12
  ON RIGHT(s12.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s12.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 12 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s13
  ON RIGHT(s13.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s13.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 13 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s14
  ON RIGHT(s14.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s14.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 14 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s15
  ON RIGHT(s15.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s15.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 15 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s16
  ON RIGHT(s16.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s16.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 16 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s17
  ON RIGHT(s17.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s17.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 17 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s18
  ON RIGHT(s18.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s18.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 18 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s19
  ON RIGHT(s19.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s19.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 19 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s20
  ON RIGHT(s20.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s20.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 20 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s21
  ON RIGHT(s21.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s21.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 21 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s22
  ON RIGHT(s22.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s22.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 22 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s23
  ON RIGHT(s23.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s23.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 23 MONTH))

LEFT JOIN `fiserv.cardholder_master_daily_snapshot` s24
  ON RIGHT(s24.ifo_curr_full_acct_no, 16) = drv.AccountID
  AND s24.as_of_date = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 24 MONTH))
where pmic_global_exclusion = 0 
;



with val as (
select a.AccountID ,a.book_vintage,  BalanceT24 , b.AcctBalTotlAmt as cc_perf_bal_m24, case when b.FiservAccountID is null then  0 else 1 end as cind
from Group_Risk.npc_acq_balance_model_step2_balance as a 
left join  `datamart_risk.cc_acct_performance_metrics` as b 
on a.AccountID = b.FiservAccountID and b.mob = 24
qualify row_number() over (partition by a.AccountID order by  b.TimePerdCd asc) = 1
)
select    case when BalanceT24 <> val.cc_perf_bal_m24 then 'NO match' else 'Match' end as ind 
,   count(distinct accountid )
from val 
where book_vintage  <='2024-03-31' and  book_vintage  >='2022-01-31' 
group by all
 ; 
--  cind	ind	f0_
-- 1	Match 355571


CREATE OR REPLACE TABLE Group_Risk.npc_acq_balance_model_step3_profile AS 
 select a.* ,   

       CASE
        WHEN a.AppChannel IN ('CK_LIGHTBOX') THEN '1.CKLB'
        WHEN a.AppChannel IN ('EXPERIAN_CRDT_MTCH') THEN '2.ECM'
        WHEN a.AppChannel IN ('DIRECT_MAIL') THEN '3.DM'
        WHEN a.AppChannel IN ('REFERRALS') THEN '4.RAF'
        ELSE '5.Other'
        END
        AS Channels,
    case       WHEN RVLR80 = "PRREVLR" THEN "Revolver"
    WHEN RVLR80 = "PRTRANS" THEN "Transactor"
    ELSE "Mixed Behavior"
    END AS revolvingBehaviorIndicator80, 
      case      WHEN b.vantagescore BETWEEN 580 AND 599 THEN '3. Sub Prime'
        WHEN b.vantagescore >= 660 THEN '1. Prime+'
        WHEN b.vantagescore BETWEEN 600 AND 659 THEN '2. Near Prime'
        WHEN b.vantagescore < 580 THEN '4. SubPrime <580' 
        ELSE '5. Check'
        END AS vband2,
      case      WHEN b.vantagescore BETWEEN 580 AND 599 THEN '1.580-599'
      WHEN b.vantagescore BETWEEN 600 AND 619 THEN '2.600-619'
      WHEN b.vantagescore BETWEEN 620 AND 639 THEN '3.620-639'
      WHEN b.vantagescore BETWEEN 640 AND 659 THEN '4.640-659'
      WHEN b.vantagescore >= 660 THEN '5.660+' 
      WHEN b.vantagescore < 580 THEN '0. <580' 
        ELSE '9. other'
        END AS vband1,     
      case when b.pmic2score >0 then b.pmic2score
           when c.pmic2score >0 then c.pmic2score else null end as pmic2score
 from Group_Risk.npc_acq_balance_model_step2_balance as a 
 left join datamart_risk.npc_funnel_detail as b 
 on a.AccountID = b.FiservAccountID
 left join  Group_Risk.PMIC2_Model_Score_Jan2022_Apr2025 as c
      on b.ExternalApplicationID = c.ExternalApplicationID
--  where b.ApplicationStatusDate >= '2022-01-01'
--  and b.ApplicationStatusDate <= '2024-03-31'  
 ;
 
 select EverCOBK_mob24 ,EverSettlement_mob24 , EverLTworkout_mob24 ,
 count(*) , 
 sum(case when mdl_balance_t24 = 0 then 1 else 0 end) as zero_bal_cnt,
 sum(case when mdl_balance_t24 < 0 then 1 else 0 end) as neg_bal_cnt,
from Group_Risk.npc_acq_balance_model_step3_profile 
where mdl_balance_t24 <= 0 and ModelTarget = 1 
group by all 
order by 1 desc,2 desc ,3 desc ; 


select count(*) , avg(mdl_balance_t24 ) as mdl_avg_bal, 
avg(BalanceT24 ) as avg_bal_t24, 
 count(*) , 
 sum(case when mdl_balance_t24 = 0 then 1 else 0 end) as zero_bal_cnt,
 sum(case when mdl_balance_t24 < 0 then 1 else 0 end) as neg_bal_cnt,
from Group_Risk.npc_acq_balance_model_step3_profile  
;

select   ModelTarget  ,count(*) , avg(mdl_balance_t24 ) as mdl_avg_bal, 
avg(BalanceT24 ) as avg_bal_t24, 
 count(*) , 
 sum(case when mdl_balance_t24 = 0 then 1 else 0 end) as zero_bal_cnt,
 sum(case when mdl_balance_t24 < 0 then 1 else 0 end) as neg_bal_cnt,
from Group_Risk.npc_acq_balance_model_step3_profile  
group by all ;

 
CREATE OR REPLACE TABLE Group_Risk.npc_acq_balance_model_step3_profile_cl  AS
SELECT
  drv.*, 
  IFNULL(s0.CreditLineAmt, 0)  AS CreditLineT0,
  IFNULL(s1.CreditLineAmt, 0)  AS CreditLineT1,
  IFNULL(s2.CreditLineAmt, 0)  AS CreditLineT2,
  IFNULL(s3.CreditLineAmt, 0)  AS CreditLineT3,
  IFNULL(s4.CreditLineAmt, 0)  AS CreditLineT4,
  IFNULL(s5.CreditLineAmt, 0)  AS CreditLineT5,
  IFNULL(s6.CreditLineAmt, 0)  AS CreditLineT6,
  IFNULL(s7.CreditLineAmt, 0)  AS CreditLineT7,
  IFNULL(s8.CreditLineAmt, 0)  AS CreditLineT8,
  IFNULL(s9.CreditLineAmt, 0)  AS CreditLineT9,
  IFNULL(s10.CreditLineAmt, 0) AS CreditLineT10,
  IFNULL(s11.CreditLineAmt, 0) AS CreditLineT11,
  IFNULL(s12.CreditLineAmt, 0) AS CreditLineT12,
  IFNULL(s13.CreditLineAmt, 0) AS CreditLineT13,
  IFNULL(s14.CreditLineAmt, 0) AS CreditLineT14,
  IFNULL(s15.CreditLineAmt, 0) AS CreditLineT15,
  IFNULL(s16.CreditLineAmt, 0) AS CreditLineT16,
  IFNULL(s17.CreditLineAmt, 0) AS CreditLineT17,
  IFNULL(s18.CreditLineAmt, 0) AS CreditLineT18,
  IFNULL(s19.CreditLineAmt, 0) AS CreditLineT19,
  IFNULL(s20.CreditLineAmt, 0) AS CreditLineT20,
  IFNULL(s21.CreditLineAmt, 0) AS CreditLineT21,
  IFNULL(s22.CreditLineAmt, 0) AS CreditLineT22,
  IFNULL(s23.CreditLineAmt, 0) AS CreditLineT23,
  IFNULL(s24.CreditLineAmt, 0) AS CreditLineT24, 

  CASE
    WHEN COALESCE(
           drv.first_cobk_mob,
           LEAST(IFNULL(drv.first_settlement_mob, 99),
                 IFNULL(drv.first_ltworkout_mob,  99))
         ) <= 24
    THEN
      CASE COALESCE(
             drv.first_cobk_mob,
             LEAST(IFNULL(drv.first_settlement_mob, 99),
                   IFNULL(drv.first_ltworkout_mob,  99))
           )
        WHEN 1  THEN IFNULL(s1.CreditLineAmt,  0)
        WHEN 2  THEN IFNULL(s2.CreditLineAmt,  0)
        WHEN 3  THEN IFNULL(s3.CreditLineAmt,  0)
        WHEN 4  THEN IFNULL(s4.CreditLineAmt,  0)
        WHEN 5  THEN IFNULL(s5.CreditLineAmt,  0)
        WHEN 6  THEN IFNULL(s6.CreditLineAmt,  0)
        WHEN 7  THEN IFNULL(s7.CreditLineAmt,  0)
        WHEN 8  THEN IFNULL(s8.CreditLineAmt,  0)
        WHEN 9  THEN IFNULL(s9.CreditLineAmt,  0)
        WHEN 10 THEN IFNULL(s10.CreditLineAmt, 0)
        WHEN 11 THEN IFNULL(s11.CreditLineAmt, 0)
        WHEN 12 THEN IFNULL(s12.CreditLineAmt, 0)
        WHEN 13 THEN IFNULL(s13.CreditLineAmt, 0)
        WHEN 14 THEN IFNULL(s14.CreditLineAmt, 0)
        WHEN 15 THEN IFNULL(s15.CreditLineAmt, 0)
        WHEN 16 THEN IFNULL(s16.CreditLineAmt, 0)
        WHEN 17 THEN IFNULL(s17.CreditLineAmt, 0)
        WHEN 18 THEN IFNULL(s18.CreditLineAmt, 0)
        WHEN 19 THEN IFNULL(s19.CreditLineAmt, 0)
        WHEN 20 THEN IFNULL(s20.CreditLineAmt, 0)
        WHEN 21 THEN IFNULL(s21.CreditLineAmt, 0)
        WHEN 22 THEN IFNULL(s22.CreditLineAmt, 0)
        WHEN 23 THEN IFNULL(s23.CreditLineAmt, 0)
        WHEN 24 THEN IFNULL(s24.CreditLineAmt, 0)
      END
    ELSE IFNULL(s24.CreditLineAmt, 0)
  END AS mdl_credit_line_t24

FROM Group_Risk.npc_acq_balance_model_step3_profile  drv

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s0
  ON s0.FiservAccountID = drv.AccountID
  AND s0.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 0 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s1
  ON s1.FiservAccountID = drv.AccountID
  AND s1.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 1 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s2
  ON s2.FiservAccountID = drv.AccountID
  AND s2.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 2 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s3
  ON s3.FiservAccountID = drv.AccountID
  AND s3.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 3 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s4
  ON s4.FiservAccountID = drv.AccountID
  AND s4.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 4 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s5
  ON s5.FiservAccountID = drv.AccountID
  AND s5.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 5 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s6
  ON s6.FiservAccountID = drv.AccountID
  AND s6.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 6 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s7
  ON s7.FiservAccountID = drv.AccountID
  AND s7.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 7 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s8
  ON s8.FiservAccountID = drv.AccountID
  AND s8.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 8 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s9
  ON s9.FiservAccountID = drv.AccountID
  AND s9.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 9 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s10
  ON s10.FiservAccountID = drv.AccountID
  AND s10.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 10 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s11
  ON s11.FiservAccountID = drv.AccountID
  AND s11.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 11 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s12
  ON s12.FiservAccountID = drv.AccountID
  AND s12.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 12 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s13
  ON s13.FiservAccountID = drv.AccountID
  AND s13.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 13 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s14
  ON s14.FiservAccountID = drv.AccountID
  AND s14.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 14 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s15
  ON s15.FiservAccountID = drv.AccountID
  AND s15.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 15 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s16
  ON s16.FiservAccountID = drv.AccountID
  AND s16.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 16 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s17
  ON s17.FiservAccountID = drv.AccountID
  AND s17.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 17 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s18
  ON s18.FiservAccountID = drv.AccountID
  AND s18.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 18 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s19
  ON s19.FiservAccountID = drv.AccountID
  AND s19.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 19 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s20
  ON s20.FiservAccountID = drv.AccountID
  AND s20.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 20 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s21
  ON s21.FiservAccountID = drv.AccountID
  AND s21.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 21 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s22
  ON s22.FiservAccountID = drv.AccountID
  AND s22.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 22 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s23
  ON s23.FiservAccountID = drv.AccountID
  AND s23.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 23 MONTH))

LEFT JOIN `datamart_risk.cc_acct_performance_metrics` s24
  ON s24.FiservAccountID = drv.AccountID
  AND s24.TimePerdCd = LAST_DAY(DATE_ADD(book_vintage, INTERVAL 24 MONTH)) 
;


