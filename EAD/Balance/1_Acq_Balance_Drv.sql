DECLARE start_date DATE DEFAULT '2022-01-01';
DECLARE end_date DATE DEFAULT '2024-03-31';

CREATE OR REPLACE TABLE Group_Risk.pmic3_2201_2403
AS
WITH
  npc AS (
    SELECT * EXCEPT (row_num)
    FROM
      (
        SELECT
          npc.ApplicationID,
          npc.GDSOfferResponseID,
          npc.CreatedDateApplication,
          FORMAT_DATE('%Y-%m', CreatedDateApplication) AS Vintage_month,
          npc.FiservAccountID,
          npc.creditLine,
          npc.pmicscore,
          npc.IsTestApplication,
          npc.FraudSuspect0123,
          npc.dtiWithHousing,
          npc.dtiWithoutHousing,
          npc.annualIncome,
          npc.additionalIncome,
          npc.abilityToPayAmount,
          npc.incomeSourceTypes,
          npc.appChannel,
          npc.campaignPartner,
          npc.CCFlow,
          npc.random01,
          npc.random02,
          npc.random03,
          npc.random04,
          npc.random05,
          npc.random06,
          npc.random07,
          npc.random08,
          npc.random09,
          npc.random10,
          npc.cardepdscore,
          npc.cardepdpullFlag,
          npc.cardepderrorMessage,
          npc.pmicpullFlag,
          npc.pmicerrorMessage,
          npc.OriginationFICOScore AS FICO,
          npc.VantageScore,
          npc.AT02S,
          npc.AT01S,
          npc.AT20S,
          npc.G106S,
          npc.S207A,
          npc.CO04S,
          npc.declineCode,
          ROW_NUMBER()
            OVER (
              PARTITION BY npc.ExternalApplicationID
              ORDER BY CreatedDateApplication DESC
            ) AS row_num
        FROM datamart_risk.npc_funnel_detail AS npc
      ) tmp
    WHERE
      1 = 1
      AND row_num = 1
      AND FiservAccountID IS NOT NULL
      AND tmp.CreatedDateApplication >= start_date
      AND tmp.CreatedDateApplication < end_date
  ),

  ------------------------------------------------------------
  -- Fraud-related lookup CTEs (reference external tables + npc)
  ------------------------------------------------------------
  everFraud AS (
    SELECT DISTINCT AcctId, 1 AS everFraud
    FROM npc
    JOIN datamart_risk.npc_utilization_tracking_nightly AS cm
      ON npc.FiservAccountID = cm.AcctId
    --- AND DATE_DIFF(cm.DataLoadDate, npc.CreatedDateApplication, MONTH) <= 24
    WHERE cm.ExternalStatus = 'Z' AND cm.CoffReasonCode = '88'
  ),
  first_party_fraud AS (
    SELECT
      AccountID, MIN(TimePeriodDate) AS first_fpf_date
    FROM `datamart_fraud.npc_ext_status_shift`
    WHERE
      ExtrSttsCd = 'C'
      AND ExtrSttsChrgOffResnCd = '36'
    GROUP BY AccountID
  ),
  synthetic_app_1st AS (
    SELECT
      AccountID, MIN(TimePeriodDate) AS first_syn1st_date
    FROM `datamart_fraud.npc_ext_status_shift`
    WHERE
      ExtrSttsCd = 'C'
      AND ExtrSttsChrgOffResnCd = '39'
    GROUP BY AccountID
  )

-- only 21 accts all happened in 2022
,
synthetic_app_3rd AS (
  SELECT
    AccountID, MIN(TimePeriodDate) AS first_syn3rd_date
  FROM `datamart_fraud.npc_ext_status_shift`
  WHERE
    ExtrSttsCd = 'C'
    AND ExtrSttsChrgOffResnCd = '40'
  GROUP BY AccountID
),
fraud_acc AS (
  SELECT
    AccountID,
    MAX(LostReportingDate) AS LostReportingDate,
    SUM(TotalFraudDisputed) AS TotalFraudDisputed,
    SUM(FraudPrincipleChargeoff) AS FraudPrincipleChargeoff
  FROM `datamart_fraud.npc_fraud_account_pi`
  WHERE
    AccountID IN (
      SELECT DISTINCT
        CASE
          WHEN Filed_Incorrectly_Y_N THEN NULL
          ELSE AccountID
          END AS AccountID
      FROM `datamart_fraud.npc_fraud_account_pi` nfap
      LEFT JOIN `Group_Risk.npc_mislabeled_fraud_apps_2023` nmfa
        ON nfap.AccountID = CAST(nmfa.Account_ID AS STRING)
      WHERE
        LostStlnTypeCD = '03'
        AND InFraudDB = 'N'
    )
  GROUP BY AccountID
),
utilization_ordered AS (
  SELECT
    utn.AcctId, MAX(DataLoadDate) AS NonNullDate
  FROM `datamart_risk.npc_utilization_tracking_nightly` utn
  WHERE
    TotalMerchant IS NOT NULL
    OR TotalCash IS NOT NULL
  GROUP BY AcctId
),
fraud_app AS (
  SELECT DISTINCT
    b.FiservAccountID,
    CASE WHEN fa.AccountID IS NOT NULL THEN 1 ELSE 0 END AS FraudAppInd,
    fa.LostReportingDate,
    fa.TotalFraudDisputed,
    fa.FraudPrincipleChargeoff,
    utn.TotalMerchant + utn.TotalCash AS TotalSalesAndCash
  FROM `data-lake-prod-223818.datamart_risk.npc_funnel_detail` b
  LEFT JOIN utilization_ordered uo
    ON b.FiservAccountID = uo.AcctId
  LEFT JOIN `datamart_risk.npc_utilization_tracking_nightly` utn
    ON
      utn.AcctId = b.FiservAccountID
      AND utn.DataLoadDate = uo.NonNullDate
  LEFT JOIN fraud_acc fa
    ON fa.AccountID = b.FiservAccountID
) ------------------------------------------------------------
  -- Exclusion criteria: 25 individual flags + rolled-up flag
  ------------------------------------------------------------
  ,
npc_with_exclusions AS (
  SELECT
    *,
    CASE
      WHEN
        excl_underage = 1
        OR excl_low_income = 1
        OR excl_recent_decline = 1
        OR excl_app_completed = 1
        OR excl_app_inprogress = 1
        OR excl_bureau_nohit = 1
        OR excl_credit_freeze = 1
        OR excl_ssn_mismatch = 1
        OR excl_dob_mismatch = 1
        OR excl_fraud_alert = 1
        OR excl_invalid_fico = 1
        OR excl_invalid_vantage = 1
        OR excl_low_fico = 1
        OR excl_no_open_trade = 1
        OR excl_low_trade_count = 1
        OR excl_low_vantage = 1
        OR excl_thin_oldest_trade = 1
        OR excl_thin_bureau = 1
        OR excl_recent_bankruptcy = 1
        OR excl_recent_chargeoff = 1
        OR excl_test_application = 1
        OR excl_ever_fraud_co = 1
        OR excl_fraud_suspect = 1
        OR excl_synthetic_3rd = 1
        OR excl_fraud_app = 1
        THEN 1
      ELSE 0
      END AS pmic_global_exclusion
  FROM
    (
      SELECT
        npc.*

          -- 1. P00010: Age < 18 (no age field in npc_funnel_detail; retained as decline code)
          ,
        CASE
          WHEN npc.declineCode = 'P00010'
            THEN 1
          ELSE 0
          END AS excl_underage

            -- 2. P00020: Annual Income < 6000
            ,
        CASE
          WHEN npc.AnnualIncome < 6000
            THEN 1
          ELSE 0
          END AS excl_low_income

            -- 3. M00001: Offer declined in last 30 days
            ,
        CASE
          WHEN npc.declineCode = 'M00001'
            THEN 1
          ELSE 0
          END AS excl_recent_decline

            -- 4. M00002: Application completed
            ,
        CASE
          WHEN npc.declineCode = 'M00002'
            THEN 1
          ELSE 0
          END AS excl_app_completed

            -- 5. M00003: Application in-progress
            ,
        CASE
          WHEN npc.declineCode = 'M00003'
            THEN 1
          ELSE 0
          END AS excl_app_inprogress

            -- 6. H00010: Customer no-hit on bureau
            ,
        CASE
          WHEN npc.declineCode = 'H00010'
            THEN 1
          ELSE 0
          END AS excl_bureau_nohit

            -- 7. F00010: Credit freeze on bureau
            ,
        CASE
          WHEN npc.declineCode = 'F00010'
            THEN 1
          ELSE 0
          END AS excl_credit_freeze

            -- 8. K00010: SSN mismatch
            ,
        CASE
          WHEN npc.declineCode = 'K00010'
            THEN 1
          ELSE 0
          END AS excl_ssn_mismatch

            -- 9. K00020: DOB mismatch
            ,
        CASE
          WHEN npc.declineCode = 'K00020'
            THEN 1
          ELSE 0
          END AS excl_dob_mismatch

            -- 10. F00020: Fraud Alert / Bankruptcy in last 7 months (S207A)
            ,
        CASE
          WHEN
            npc.S207A IS NOT NULL
            AND CAST(npc.S207A AS INT64) >= 0
            AND CAST(npc.S207A AS INT64) <= 7
            THEN 1
          ELSE 0
          END AS excl_fraud_alert

            -- 11. Invalid or Null FICO (OriginationFICOScore aliased as FICO)
            ,
        CASE
          WHEN
            npc.FICO IS NULL
            OR npc.FICO < 300
            OR npc.FICO > 850
            THEN 1
          ELSE 0
          END AS excl_invalid_fico

            -- 12. Invalid or Null Vantage 4.0 Score (VantageScore)
            ,
        CASE
          WHEN
            npc.VantageScore IS NULL
            OR CAST(npc.VantageScore AS INT64) < 300
            OR CAST(npc.VantageScore AS INT64) > 850
            THEN 1
          ELSE 0
          END AS excl_invalid_vantage

            -- 13. FICO < 540
            ,
        CASE
          WHEN npc.FICO < 540
            THEN 1
          ELSE 0
          END AS excl_low_fico

            -- 14. No Open Trade: AT02S is null or = 0
            ,
        CASE
          WHEN
            npc.AT02S IS NULL
            OR CAST(npc.AT02S AS INT64) = 0
            THEN 1
          ELSE 0
          END AS excl_no_open_trade

            -- 15. Total Number of Trades < 2: AT01S < 2
            ,
        CASE
          WHEN CAST(npc.AT01S AS INT64) < 2
            THEN 1
          ELSE 0
          END AS excl_low_trade_count

            -- 16. Vantage 4.0 Score < 540 (VantageScore)
            ,
        CASE
          WHEN CAST(npc.VantageScore AS INT64) < 540
            THEN 1
          ELSE 0
          END AS excl_low_vantage

            -- 17. Age of Oldest Trade <= 12 months: AT20S
            ,
        CASE
          WHEN
            npc.AT20S IS NULL
            OR CAST(npc.AT20S AS INT64) <= 12
            THEN 1
          ELSE 0
          END AS excl_thin_oldest_trade

            -- 18. Months on Bureau <= 12 months: G106S
            ,
        CASE
          WHEN
            npc.G106S IS NULL
            OR CAST(npc.G106S AS INT64) <= 12
            THEN 1
          ELSE 0
          END AS excl_thin_bureau

            -- 19. Bankruptcy in last 7 months: S207A
            ,
        CASE
          WHEN
            npc.S207A IS NOT NULL
            AND CAST(npc.S207A AS INT64) >= 0
            AND CAST(npc.S207A AS INT64) <= 7
            THEN 1
          ELSE 0
          END AS excl_recent_bankruptcy

            -- 20. Charge-off in last 4 months: CO04S
            ,
        CASE
          WHEN
            npc.CO04S IS NOT NULL
            AND CAST(npc.CO04S AS INT64) >= 0
            AND CAST(npc.CO04S AS INT64) <= 4
            THEN 1
          ELSE 0
          END AS excl_recent_chargeoff

            -- 21. Test Applications
            ,
        CASE
          WHEN npc.IsTestApplication = TRUE
            THEN 1
          ELSE 0
          END AS excl_test_application

            -- 22. Fraud: booked Z88(fraud chargeoff) confirmed fraud accounts
            ,
        CASE
          WHEN ef.everFraud = 1
            THEN 1
          ELSE 0
          END AS excl_ever_fraud_co

            -- 23. Fraud attack accounts: FraudSuspect0123 != 'Non Fraud'
            ,
        CASE
          WHEN npc.FraudSuspect0123 != 'Non Fraud'
            THEN 1
          ELSE 0
          END AS excl_fraud_suspect

            -- 24. Synthetic 3rd-party fraud app: first_syn3rd_date IS NOT NULL
            ,
        CASE
          WHEN sa_3.first_syn3rd_date IS NOT NULL
            THEN 1
          ELSE 0
          END AS excl_synthetic_3rd

            -- 25. Fraud Apps: agent-confirmed-- Largely overlaps with item 22; non-overlapped are accts not in DPD status or in DPD but before charge-off
            ,
        CASE WHEN fa.FraudAppInd = 1 THEN 1 ELSE 0 END AS excl_fraud_app
      FROM npc
      LEFT JOIN everFraud AS ef
        ON npc.FiservAccountID = ef.AcctId
      LEFT JOIN synthetic_app_3rd AS sa_3
        ON npc.FiservAccountID = sa_3.AccountID
      LEFT JOIN fraud_app AS fa
        ON npc.FiservAccountID = fa.FiservAccountID
    ) AS flags
),

------------------------------------------------------------
-- Per-account per-MOB event flags from utilization tracking
-- Uses the Mob column already in the table
-- no cobk, settlement or ltworkout happened in mob0 so the mob range is 1<=mob<=24
------------------------------------------------------------
mob_events AS (
  SELECT
    npc.FiservAccountID,
    cm.Mob,
    MAX(
      CASE
        WHEN cm.ExternalStatus = 'B' OR cm.ExternalStatus = 'Z' THEN 1
        ELSE 0
        END) AS cobk_at_mob,
    MAX(
      CASE WHEN cm.CoffReasonCode IN ('68', '43', '47', '57') THEN 1 ELSE 0 END)
      AS settlement_at_mob,
    MAX(CASE WHEN cm.CoffReasonCode = '46' THEN 1 ELSE 0 END)
      AS ltworkout_at_mob
  FROM npc_with_exclusions AS npc
  JOIN datamart_risk.npc_utilization_tracking_nightly AS cm
    ON npc.FiservAccountID = cm.AcctId
  WHERE cm.Mob BETWEEN 1 AND 24
  GROUP BY 1, 2
),

------------------------------------------------------------
-- Point-in-time per-MOB flags via conditional aggregation
-- Each flag = 1 only if the event happened AT that MOB
------------------------------------------------------------
mob_pivot AS (
  SELECT
    FiservAccountID

      -- CO/BK at MOB n (point-in-time)
      ,
    MAX(CASE WHEN Mob = 1 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob1,
    MAX(CASE WHEN Mob = 2 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob2,
    MAX(CASE WHEN Mob = 3 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob3,
    MAX(CASE WHEN Mob = 4 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob4,
    MAX(CASE WHEN Mob = 5 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob5,
    MAX(CASE WHEN Mob = 6 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob6,
    MAX(CASE WHEN Mob = 7 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob7,
    MAX(CASE WHEN Mob = 8 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob8,
    MAX(CASE WHEN Mob = 9 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob9,
    MAX(CASE WHEN Mob = 10 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob10,
    MAX(CASE WHEN Mob = 11 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob11,
    MAX(CASE WHEN Mob = 12 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob12,
    MAX(CASE WHEN Mob = 13 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob13,
    MAX(CASE WHEN Mob = 14 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob14,
    MAX(CASE WHEN Mob = 15 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob15,
    MAX(CASE WHEN Mob = 16 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob16,
    MAX(CASE WHEN Mob = 17 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob17,
    MAX(CASE WHEN Mob = 18 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob18,
    MAX(CASE WHEN Mob = 19 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob19,
    MAX(CASE WHEN Mob = 20 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob20,
    MAX(CASE WHEN Mob = 21 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob21,
    MAX(CASE WHEN Mob = 22 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob22,
    MAX(CASE WHEN Mob = 23 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob23,
    MAX(CASE WHEN Mob = 24 AND cobk_at_mob = 1 THEN 1 ELSE 0 END) AS cobk_mob24

        -- Settlement at MOB n (point-in-time)
        ,
    MAX(CASE WHEN Mob = 1 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob1,
    MAX(CASE WHEN Mob = 2 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob2,
    MAX(CASE WHEN Mob = 3 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob3,
    MAX(CASE WHEN Mob = 4 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob4,
    MAX(CASE WHEN Mob = 5 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob5,
    MAX(CASE WHEN Mob = 6 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob6,
    MAX(CASE WHEN Mob = 7 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob7,
    MAX(CASE WHEN Mob = 8 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob8,
    MAX(CASE WHEN Mob = 9 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob9,
    MAX(CASE WHEN Mob = 10 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob10,
    MAX(CASE WHEN Mob = 11 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob11,
    MAX(CASE WHEN Mob = 12 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob12,
    MAX(CASE WHEN Mob = 13 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob13,
    MAX(CASE WHEN Mob = 14 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob14,
    MAX(CASE WHEN Mob = 15 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob15,
    MAX(CASE WHEN Mob = 16 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob16,
    MAX(CASE WHEN Mob = 17 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob17,
    MAX(CASE WHEN Mob = 18 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob18,
    MAX(CASE WHEN Mob = 19 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob19,
    MAX(CASE WHEN Mob = 20 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob20,
    MAX(CASE WHEN Mob = 21 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob21,
    MAX(CASE WHEN Mob = 22 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob22,
    MAX(CASE WHEN Mob = 23 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob23,
    MAX(CASE WHEN Mob = 24 AND settlement_at_mob = 1 THEN 1 ELSE 0 END)
      AS settlement_mob24

        -- LT Workout at MOB n (point-in-time)
        ,
    MAX(CASE WHEN Mob = 1 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob1,
    MAX(CASE WHEN Mob = 2 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob2,
    MAX(CASE WHEN Mob = 3 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob3,
    MAX(CASE WHEN Mob = 4 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob4,
    MAX(CASE WHEN Mob = 5 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob5,
    MAX(CASE WHEN Mob = 6 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob6,
    MAX(CASE WHEN Mob = 7 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob7,
    MAX(CASE WHEN Mob = 8 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob8,
    MAX(CASE WHEN Mob = 9 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob9,
    MAX(CASE WHEN Mob = 10 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob10,
    MAX(CASE WHEN Mob = 11 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob11,
    MAX(CASE WHEN Mob = 12 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob12,
    MAX(CASE WHEN Mob = 13 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob13,
    MAX(CASE WHEN Mob = 14 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob14,
    MAX(CASE WHEN Mob = 15 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob15,
    MAX(CASE WHEN Mob = 16 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob16,
    MAX(CASE WHEN Mob = 17 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob17,
    MAX(CASE WHEN Mob = 18 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob18,
    MAX(CASE WHEN Mob = 19 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob19,
    MAX(CASE WHEN Mob = 20 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob20,
    MAX(CASE WHEN Mob = 21 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob21,
    MAX(CASE WHEN Mob = 22 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob22,
    MAX(CASE WHEN Mob = 23 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob23,
    MAX(CASE WHEN Mob = 24 AND ltworkout_at_mob = 1 THEN 1 ELSE 0 END)
      AS ltworkout_mob24

        -- First MOB per individual bad definition
        ,
    MIN(CASE WHEN cobk_at_mob = 1 THEN Mob END) AS first_cobk_mob,
    MIN(CASE WHEN settlement_at_mob = 1 THEN Mob END) AS first_settlement_mob,
    MIN(CASE WHEN ltworkout_at_mob = 1 THEN Mob END) AS first_ltworkout_mob,
    MIN(
      CASE
        WHEN cobk_at_mob = 1 OR settlement_at_mob = 1 OR ltworkout_at_mob = 1
          THEN Mob
        END) AS first_bad_mob_status
  FROM mob_events
  GROUP BY FiservAccountID
),

------------------------------------------------------------
-- DPD by MOB from cardholder master portfolio + delinquency
-- Month-end snapshots only
------------------------------------------------------------
dpd_mob24 AS (
  SELECT * EXCEPT (MonthEndFlag)
  FROM
    (
      SELECT
        RIGHT(ifo_base_full_acct_no, 16) AS AcctID,
        ifo_open_date AS AcctOpenDate,
        ifo_external_status AS ExternalStatus,
        CASE
          WHEN t2.ifo_current_balance IS NULL THEN 0
          ELSE t2.ifo_current_balance
          END AS AcctBalTotal,
        DATE_DIFF(t1.as_of_date, ifo_open_date, MONTH) AS Mob,
        CASE
          WHEN ifo_del_no_days - 29 < 0 THEN 0
          ELSE ifo_del_no_days - 29
          END AS Dpd,
        CASE WHEN ifo_del_no_days - 29 >= 15 THEN 1 ELSE 0 END AS Dpd15Plus,
        CASE WHEN ifo_del_no_days - 29 >= 30 THEN 1 ELSE 0 END AS Dpd30Plus,
        CASE WHEN ifo_del_no_days - 29 >= 45 THEN 1 ELSE 0 END AS Dpd45Plus,
        CASE WHEN ifo_del_no_days - 29 >= 60 THEN 1 ELSE 0 END AS Dpd60Plus,
        CASE WHEN ifo_del_no_days - 29 >= 90 THEN 1 ELSE 0 END AS Dpd90Plus,
        t1.as_of_date AS SnapshotDate,
        IF(t1.as_of_date = (LAST_DAY(t1.as_of_date)), 1, 0) AS MonthEndFlag
      FROM datamart_card.cardholder_master_portfolio_daily_snapshot t1
      LEFT JOIN fiserv.cardholder_master_daily_snapshot t2
        ON
          t1.ifo_base_full_acct_no = t2.ifo_curr_full_acct_no
          AND t1.as_of_date = t2.as_of_date
    )
  WHERE
    1 = 1
    AND MonthEndFlag = 1
),

------------------------------------------------------------
-- First MOB where DPD 90+ from cardholder master snapshots
------------------------------------------------------------
first_dpd90 AS (
  SELECT
    AcctID, MIN(Mob) AS first_dpd90_mob
  FROM dpd_mob24
  WHERE Dpd >= 90
  GROUP BY AcctID
),

------------------------------------------------------------
-- Never-active accounts
------------------------------------------------------------
never_active AS (
  SELECT
    AcctID, 1 AS never_active
  FROM
    datamart_risk.npc_utilization_tracking_nightly
  WHERE
    mob <= 24
  GROUP BY
    AcctID
  HAVING MAX(
    IfActiveInd) = 0  -- Never hit 1 at any point up to MOB 24
    AND COUNT(DISTINCT mob)
      = 25  /* Ensures all snapshots (0 through 24) exist*/
) ------------------------------------------------------------
  -- Combine base population with performance flags
  -- only one edge case have AcctOpenDate is null --fiservAccountID=0000000000000000
  ------------------------------------------------------------
  ,
base_with_flags AS (
  SELECT
    npc.*,
    d.AcctOpenDate,
    last_day(d.AcctOpenDate) AS book_vintage
        -- bad_mob1 through bad_mob24: point-in-time bad indicator AT each MOB
        -- Fraud flags only contribute at the specific MOB when they occurred (based on date)
        ,
    GREATEST(
      COALESCE(mp.cobk_mob1, 0),
      COALESCE(mp.settlement_mob1, 0),
      COALESCE(mp.ltworkout_mob1, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 1 THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 1 THEN 1
        ELSE 0
        END) AS bad_mob1,
    GREATEST(
      COALESCE(mp.cobk_mob2, 0),
      COALESCE(mp.settlement_mob2, 0),
      COALESCE(mp.ltworkout_mob2, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 2 THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 2 THEN 1
        ELSE 0
        END) AS bad_mob2,
    GREATEST(
      COALESCE(mp.cobk_mob3, 0),
      COALESCE(mp.settlement_mob3, 0),
      COALESCE(mp.ltworkout_mob3, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 3 THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 3 THEN 1
        ELSE 0
        END) AS bad_mob3,
    GREATEST(
      COALESCE(mp.cobk_mob4, 0),
      COALESCE(mp.settlement_mob4, 0),
      COALESCE(mp.ltworkout_mob4, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 4 THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 4 THEN 1
        ELSE 0
        END) AS bad_mob4,
    GREATEST(
      COALESCE(mp.cobk_mob5, 0),
      COALESCE(mp.settlement_mob5, 0),
      COALESCE(mp.ltworkout_mob5, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 5 THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 5 THEN 1
        ELSE 0
        END) AS bad_mob5,
    GREATEST(
      COALESCE(mp.cobk_mob6, 0),
      COALESCE(mp.settlement_mob6, 0),
      COALESCE(mp.ltworkout_mob6, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 6 THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 6 THEN 1
        ELSE 0
        END) AS bad_mob6,
    GREATEST(
      COALESCE(mp.cobk_mob7, 0),
      COALESCE(mp.settlement_mob7, 0),
      COALESCE(mp.ltworkout_mob7, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 7 THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 7 THEN 1
        ELSE 0
        END) AS bad_mob7,
    GREATEST(
      COALESCE(mp.cobk_mob8, 0),
      COALESCE(mp.settlement_mob8, 0),
      COALESCE(mp.ltworkout_mob8, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 8 THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 8 THEN 1
        ELSE 0
        END) AS bad_mob8,
    GREATEST(
      COALESCE(mp.cobk_mob9, 0),
      COALESCE(mp.settlement_mob9, 0),
      COALESCE(mp.ltworkout_mob9, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 9 THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 9 THEN 1
        ELSE 0
        END) AS bad_mob9,
    GREATEST(
      COALESCE(mp.cobk_mob10, 0),
      COALESCE(mp.settlement_mob10, 0),
      COALESCE(mp.ltworkout_mob10, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 10
          THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 10 THEN 1
        ELSE 0
        END) AS bad_mob10,
    GREATEST(
      COALESCE(mp.cobk_mob11, 0),
      COALESCE(mp.settlement_mob11, 0),
      COALESCE(mp.ltworkout_mob11, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 11
          THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 11 THEN 1
        ELSE 0
        END) AS bad_mob11,
    GREATEST(
      COALESCE(mp.cobk_mob12, 0),
      COALESCE(mp.settlement_mob12, 0),
      COALESCE(mp.ltworkout_mob12, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 12
          THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 12 THEN 1
        ELSE 0
        END) AS bad_mob12,
    GREATEST(
      COALESCE(mp.cobk_mob13, 0),
      COALESCE(mp.settlement_mob13, 0),
      COALESCE(mp.ltworkout_mob13, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 13
          THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 13 THEN 1
        ELSE 0
        END) AS bad_mob13,
    GREATEST(
      COALESCE(mp.cobk_mob14, 0),
      COALESCE(mp.settlement_mob14, 0),
      COALESCE(mp.ltworkout_mob14, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 14
          THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 14 THEN 1
        ELSE 0
        END) AS bad_mob14,
    GREATEST(
      COALESCE(mp.cobk_mob15, 0),
      COALESCE(mp.settlement_mob15, 0),
      COALESCE(mp.ltworkout_mob15, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 15
          THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 15 THEN 1
        ELSE 0
        END) AS bad_mob15,
    GREATEST(
      COALESCE(mp.cobk_mob16, 0),
      COALESCE(mp.settlement_mob16, 0),
      COALESCE(mp.ltworkout_mob16, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 16
          THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 16 THEN 1
        ELSE 0
        END) AS bad_mob16,
    GREATEST(
      COALESCE(mp.cobk_mob17, 0),
      COALESCE(mp.settlement_mob17, 0),
      COALESCE(mp.ltworkout_mob17, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 17
          THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 17 THEN 1
        ELSE 0
        END) AS bad_mob17,
    GREATEST(
      COALESCE(mp.cobk_mob18, 0),
      COALESCE(mp.settlement_mob18, 0),
      COALESCE(mp.ltworkout_mob18, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 18
          THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 18 THEN 1
        ELSE 0
        END) AS bad_mob18,
    GREATEST(
      COALESCE(mp.cobk_mob19, 0),
      COALESCE(mp.settlement_mob19, 0),
      COALESCE(mp.ltworkout_mob19, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 19
          THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 19 THEN 1
        ELSE 0
        END) AS bad_mob19,
    GREATEST(
      COALESCE(mp.cobk_mob20, 0),
      COALESCE(mp.settlement_mob20, 0),
      COALESCE(mp.ltworkout_mob20, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 20
          THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 20 THEN 1
        ELSE 0
        END) AS bad_mob20,
    GREATEST(
      COALESCE(mp.cobk_mob21, 0),
      COALESCE(mp.settlement_mob21, 0),
      COALESCE(mp.ltworkout_mob21, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 21
          THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 21 THEN 1
        ELSE 0
        END) AS bad_mob21,
    GREATEST(
      COALESCE(mp.cobk_mob22, 0),
      COALESCE(mp.settlement_mob22, 0),
      COALESCE(mp.ltworkout_mob22, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 22
          THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 22 THEN 1
        ELSE 0
        END) AS bad_mob22,
    GREATEST(
      COALESCE(mp.cobk_mob23, 0),
      COALESCE(mp.settlement_mob23, 0),
      COALESCE(mp.ltworkout_mob23, 0),
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 23
          THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 23 THEN 1
        ELSE 0
        END) AS bad_mob23,
    GREATEST(
      COALESCE(mp.cobk_mob24, 0),
      COALESCE(mp.settlement_mob24, 0),
      COALESCE(mp.ltworkout_mob24, 0),
      CASE WHEN d.Dpd >= 90 THEN 1 ELSE 0 END,
      CASE
        WHEN DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) = 24
          THEN 1
        ELSE 0
        END,
      CASE
        WHEN DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) = 24 THEN 1
        ELSE 0
        END) AS bad_mob24

        -- Summary "ever" flags across full 24-month window (for QC / waterfall)
        ,
    CASE
      WHEN
        GREATEST(
          COALESCE(mp.cobk_mob1, 0),
          COALESCE(mp.cobk_mob2, 0),
          COALESCE(mp.cobk_mob3, 0),
          COALESCE(mp.cobk_mob4, 0),
          COALESCE(mp.cobk_mob5, 0),
          COALESCE(mp.cobk_mob6, 0),
          COALESCE(mp.cobk_mob7, 0),
          COALESCE(mp.cobk_mob8, 0),
          COALESCE(mp.cobk_mob9, 0),
          COALESCE(mp.cobk_mob10, 0),
          COALESCE(mp.cobk_mob11, 0),
          COALESCE(mp.cobk_mob12, 0),
          COALESCE(mp.cobk_mob13, 0),
          COALESCE(mp.cobk_mob14, 0),
          COALESCE(mp.cobk_mob15, 0),
          COALESCE(mp.cobk_mob16, 0),
          COALESCE(mp.cobk_mob17, 0),
          COALESCE(mp.cobk_mob18, 0),
          COALESCE(mp.cobk_mob19, 0),
          COALESCE(mp.cobk_mob20, 0),
          COALESCE(mp.cobk_mob21, 0),
          COALESCE(mp.cobk_mob22, 0),
          COALESCE(mp.cobk_mob23, 0),
          COALESCE(mp.cobk_mob24, 0))
        = 1
        THEN 1
      ELSE 0
      END AS EverCOBK_mob24,
    CASE
      WHEN
        GREATEST(
          COALESCE(mp.settlement_mob1, 0),
          COALESCE(mp.settlement_mob2, 0),
          COALESCE(mp.settlement_mob3, 0),
          COALESCE(mp.settlement_mob4, 0),
          COALESCE(mp.settlement_mob5, 0),
          COALESCE(mp.settlement_mob6, 0),
          COALESCE(mp.settlement_mob7, 0),
          COALESCE(mp.settlement_mob8, 0),
          COALESCE(mp.settlement_mob9, 0),
          COALESCE(mp.settlement_mob10, 0),
          COALESCE(mp.settlement_mob11, 0),
          COALESCE(mp.settlement_mob12, 0),
          COALESCE(mp.settlement_mob13, 0),
          COALESCE(mp.settlement_mob14, 0),
          COALESCE(mp.settlement_mob15, 0),
          COALESCE(mp.settlement_mob16, 0),
          COALESCE(mp.settlement_mob17, 0),
          COALESCE(mp.settlement_mob18, 0),
          COALESCE(mp.settlement_mob19, 0),
          COALESCE(mp.settlement_mob20, 0),
          COALESCE(mp.settlement_mob21, 0),
          COALESCE(mp.settlement_mob22, 0),
          COALESCE(mp.settlement_mob23, 0),
          COALESCE(mp.settlement_mob24, 0))
        = 1
        THEN 1
      ELSE 0
      END AS EverSettlement_mob24,
    CASE
      WHEN
        GREATEST(
          COALESCE(mp.ltworkout_mob1, 0),
          COALESCE(mp.ltworkout_mob2, 0),
          COALESCE(mp.ltworkout_mob3, 0),
          COALESCE(mp.ltworkout_mob4, 0),
          COALESCE(mp.ltworkout_mob5, 0),
          COALESCE(mp.ltworkout_mob6, 0),
          COALESCE(mp.ltworkout_mob7, 0),
          COALESCE(mp.ltworkout_mob8, 0),
          COALESCE(mp.ltworkout_mob9, 0),
          COALESCE(mp.ltworkout_mob10, 0),
          COALESCE(mp.ltworkout_mob11, 0),
          COALESCE(mp.ltworkout_mob12, 0),
          COALESCE(mp.ltworkout_mob13, 0),
          COALESCE(mp.ltworkout_mob14, 0),
          COALESCE(mp.ltworkout_mob15, 0),
          COALESCE(mp.ltworkout_mob16, 0),
          COALESCE(mp.ltworkout_mob17, 0),
          COALESCE(mp.ltworkout_mob18, 0),
          COALESCE(mp.ltworkout_mob19, 0),
          COALESCE(mp.ltworkout_mob20, 0),
          COALESCE(mp.ltworkout_mob21, 0),
          COALESCE(mp.ltworkout_mob22, 0),
          COALESCE(mp.ltworkout_mob23, 0),
          COALESCE(mp.ltworkout_mob24, 0))
        = 1
        THEN 1
      ELSE 0
      END AS EverLTworkout_mob24,
    CASE
      WHEN d.Dpd >= 90 THEN 1
      ELSE 0
      END AS Dpd90Plus_at_mob24

        -- First-party fraud ever flags (1 if flagged at any time within 24 MOBs)
        ,
    CASE
      WHEN
        sa_1.first_syn1st_date IS NOT NULL
        AND DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH) <= 24
        THEN 1
      ELSE 0
      END AS EverSynthetic1st_mob24,
    CASE
      WHEN
        fpf.first_fpf_date IS NOT NULL
        AND DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH) <= 24
        THEN 1
      ELSE 0
      END AS EverFirstPartyFraud_mob24

        -- First bad MOB per individual definition
        ,
    mp.first_cobk_mob,
    mp.first_settlement_mob,
    mp.first_ltworkout_mob,
    fd.first_dpd90_mob,
    mp.first_bad_mob_status

      -- Calculate MOB when fraud occurred
      ,
    DATE_DIFF(sa_1.first_syn1st_date, d.AcctOpenDate, MONTH)
      AS synthetic_app_1st_mob,
    DATE_DIFF(fpf.first_fpf_date, d.AcctOpenDate, MONTH)
      AS first_party_fraud_1st_mob
  FROM npc_with_exclusions AS npc
  LEFT JOIN mob_pivot AS mp
    ON npc.FiservAccountID = mp.FiservAccountID
  LEFT JOIN dpd_mob24 AS d
    ON
      npc.FiservAccountID = d.AcctID
      AND d.Mob = 24
  LEFT JOIN first_dpd90 AS fd
    ON npc.FiservAccountID = fd.AcctID
  LEFT JOIN first_party_fraud AS fpf
    ON npc.FiservAccountID = fpf.AccountID
  LEFT JOIN synthetic_app_1st AS sa_1
    ON npc.FiservAccountID = sa_1.AccountID
)

------------------------------------------------------------
-- Final output: add first_bad_mob and ModelTarget
------------------------------------------------------------
SELECT
  * -- Survival time for PD bad population
    , FiservAccountID as  AccountID, 
  CASE
    WHEN first_bad_mob_status IS NOT NULL THEN first_bad_mob_status
    WHEN Dpd90Plus_at_mob24 = 1 THEN 24
    WHEN EverSynthetic1st_mob24 = 1 THEN synthetic_app_1st_mob
    WHEN EverFirstPartyFraud_mob24 = 1 THEN first_party_fraud_1st_mob
    ELSE NULL
    END AS first_bad_mob,
  GREATEST(
    bad_mob1,
    bad_mob2,
    bad_mob3,
    bad_mob4,
    bad_mob5,
    bad_mob6,
    bad_mob7,
    bad_mob8,
    bad_mob9,
    bad_mob10,
    bad_mob11,
    bad_mob12,
    bad_mob13,
    bad_mob14,
    bad_mob15,
    bad_mob16,
    bad_mob17,
    bad_mob18,
    bad_mob19,
    bad_mob20,
    bad_mob21,
    bad_mob22,
    bad_mob23,
    bad_mob24) AS ModelTarget
FROM base_with_flags;

------------------------------------------------------------
-- QC: volume and bad rates by origination month
------------------------------------------------------------
SELECT
  vintage_month,
  COUNT(*) AS accounts,
  SUM(ModelTarget) AS bad_count,
  SAFE_DIVIDE(SUM(ModelTarget), COUNT(*)) AS bad_rate,
  SUM(EverCOBK_mob24) AS ever_cobk,
  SUM(EverSettlement_mob24) AS ever_settlement,
  SUM(EverLTworkout_mob24) AS ever_ltworkout,
  SUM(Dpd90Plus_at_mob24) AS dpd90_at24,
  SUM(EverSynthetic1st_mob24) AS ever_synthetic1st,
  SUM(EverFirstPartyFraud_mob24) AS ever_first_party_fraud
FROM Group_Risk.pmic3_booked_target_v2
GROUP BY 1
ORDER BY 1;

-- QC
-- accounts	total_excluded	accts_post_exc	EverCOBK	EverSettlement_wf	EverLTworkout_wf	Dpd90_at24_wf	EverSynthetic1st_wf	EverFirstPartyFraud_wf	ModelTarget
-- 364878	9219	355659	96147	10296	129	11467	38	707	118626
SELECT
  COUNT(*) AS accounts,
  SUM(pmic_global_exclusion) AS total_excluded,
  COUNT(*) - SUM(pmic_global_exclusion) AS accts_post_exc,
  SUM(EverCOBK_mob24) AS EverCOBK,
  SUM(CASE WHEN EverCOBK_mob24 = 1 THEN 0 ELSE EverSettlement_mob24 END)
    AS EverSettlement_wf,
  SUM(
    CASE
      WHEN EverCOBK_mob24 = 1 OR EverSettlement_mob24 = 1 THEN 0
      ELSE EverLTworkout_mob24
      END) AS EverLTworkout_wf,
  SUM(
    CASE
      WHEN
        EverCOBK_mob24 = 1
        OR EverSettlement_mob24 = 1
        OR EverLTworkout_mob24 = 1
        THEN 0
      ELSE Dpd90Plus_at_mob24
      END) AS Dpd90_at24_wf,
  SUM(
    CASE
      WHEN
        EverCOBK_mob24 = 1
        OR EverSettlement_mob24 = 1
        OR EverLTworkout_mob24 = 1
        OR Dpd90Plus_at_mob24 = 1
        THEN 0
      ELSE EverSynthetic1st_mob24
      END) AS EverSynthetic1st_wf,
  SUM(
    CASE
      WHEN
        EverCOBK_mob24 = 1
        OR EverSettlement_mob24 = 1
        OR EverLTworkout_mob24 = 1
        OR Dpd90Plus_at_mob24 = 1
        OR EverSynthetic1st_mob24 = 1
        THEN 0
      ELSE EverFirstPartyFraud_mob24
      END) AS EverFirstPartyFraud_wf,
  SUM(ModelTarget) AS ModelTarget
FROM Group_Risk.pmic3_2201_2403;

------------------------------------------------------------
-- QC: waterfall — mutually exclusive target components
------------------------------------------------------------
SELECT
  vintage_month,
  COUNT(*) AS accounts,
  SUM(EverCOBK_mob24) AS EverCOBK,
  SUM(CASE WHEN EverCOBK_mob24 = 1 THEN 0 ELSE EverSettlement_mob24 END)
    AS EverSettlement_wf,
  SUM(
    CASE
      WHEN EverCOBK_mob24 = 1 OR EverSettlement_mob24 = 1 THEN 0
      ELSE EverLTworkout_mob24
      END) AS EverLTworkout_wf,
  SUM(
    CASE
      WHEN
        EverCOBK_mob24 = 1
        OR EverSettlement_mob24 = 1
        OR EverLTworkout_mob24 = 1
        THEN 0
      ELSE Dpd90Plus_at_mob24
      END) AS Dpd90_at24_wf,
  SUM(
    CASE
      WHEN
        EverCOBK_mob24 = 1
        OR EverSettlement_mob24 = 1
        OR EverLTworkout_mob24 = 1
        OR Dpd90Plus_at_mob24 = 1
        THEN 0
      ELSE EverSynthetic1st_mob24
      END) AS EverSynthetic1st_wf,
  SUM(
    CASE
      WHEN
        EverCOBK_mob24 = 1
        OR EverSettlement_mob24 = 1
        OR EverLTworkout_mob24 = 1
        OR Dpd90Plus_at_mob24 = 1
        OR EverSynthetic1st_mob24 = 1
        THEN 0
      ELSE EverFirstPartyFraud_mob24
      END) AS EverFirstPartyFraud_wf,
  SUM(ModelTarget) AS ModelTarget
FROM Group_Risk.pmic3_2201_2403
GROUP BY 1
ORDER BY 1;

------------------------------------------------------------
-- QC: exclusion flag breakdown by vintage month
------------------------------------------------------------
SELECT
  Vintage_month,
  SUM(excl_underage) AS excl_underage,
  SUM(excl_low_income) AS excl_low_income,
  SUM(excl_recent_decline) AS excl_recent_decline,
  SUM(excl_app_completed) AS excl_app_completed,
  SUM(excl_app_inprogress) AS excl_app_inprogress,
  SUM(excl_bureau_nohit) AS excl_bureau_nohit,
  SUM(excl_credit_freeze) AS excl_credit_freeze,
  SUM(excl_ssn_mismatch) AS excl_ssn_mismatch,
  SUM(excl_dob_mismatch) AS excl_dob_mismatch,
  SUM(excl_fraud_alert) AS excl_fraud_alert,
  SUM(excl_invalid_fico) AS excl_invalid_fico,
  SUM(excl_invalid_vantage) AS excl_invalid_vantage,
  SUM(excl_low_fico) AS excl_low_fico,
  SUM(excl_no_open_trade) AS excl_no_open_trade,
  SUM(excl_low_trade_count) AS excl_low_trade_count,
  SUM(excl_low_vantage) AS excl_low_vantage,
  SUM(excl_thin_oldest_trade) AS excl_thin_oldest_trade,
  SUM(excl_thin_bureau) AS excl_thin_bureau,
  SUM(excl_recent_bankruptcy) AS excl_recent_bankruptcy,
  SUM(excl_recent_chargeoff) AS excl_recent_chargeoff,
  SUM(excl_test_application) AS excl_test_application,
  SUM(excl_ever_fraud_co) AS excl_ever_fraud_co,
  SUM(excl_fraud_suspect) AS excl_fraud_suspect,
  SUM(excl_synthetic_3rd) AS excl_synthetic_3rd,
  SUM(excl_fraud_app) AS excl_fraud_app,
  SUM(pmic_global_exclusion) AS total_excluded
FROM Group_Risk.pmic3_2201_2403
GROUP BY 1
ORDER BY 1;
