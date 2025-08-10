with cte as (
select (select db_stat_date-1 from tbaadm.gct) StartDate,
(select add_months(db_stat_date,84) from tbaadm.gct)EndDate,
FORACID, ACCT_NAME, SCHM_CODE, CLR_BAL_AMT, PRINCIPAL_OUTS, INTEREST_OVERDUES,
PENAL_INT_OUTS, INDEM_DEFFERED_AMT, PIDEM_DEFFERED_AMT, PRDEM_DEFFERED_AMT,ACCT_OPN_DATE,acct_CRNCY_CODE,HOLIDAY_CALENDAR,rate,FINACLECPTYCIFID, FINACLECPTYNAME, FINACLECPTYTYPE
from (

select foracid,acct_name,schm_code,clr_bal_amt,ACCT_OPN_DATE,acct_CRNCY_CODE,HOLIDAY_CALENDAR,(select nvl((select PREF_RATE_PCNT from tbaadm.ITCP where ENTITY_ID=gam.acid and ENTITY_TYPE='ACCNT'
and PREF_TYPE='ACCT' and  PREF_SUB_TYPE='C'
and INT_PREF_SRL_NUM in  (select max(INT_PREF_SRL_NUM) from tbaadm.ITCP where ENTITY_ID=gam.acid and ENTITY_TYPE='ACCNT'
and PREF_TYPE='ACCT' and  PREF_SUB_TYPE='C')
and rownum=1),0) +
nvl((select PREF_RATE_PCNT from tbaadm.ITCP where ENTITY_ID=gam.acid and ENTITY_TYPE='ACCNT'
and PREF_TYPE='CUST' and  PREF_SUB_TYPE='C'
and INT_PREF_SRL_NUM in  (select max(INT_PREF_SRL_NUM) from tbaadm.ITCP where ENTITY_ID=gam.acid and ENTITY_TYPE='ACCNT'
and PREF_TYPE='CUST' and  PREF_SUB_TYPE='C')
and rownum=1),0)
from dual) RATE,
(SELECT
NVL(PRINCIPAL_DMD,0) - NVL(PRINCIPAL_PAID,0) PRINCIPAL_OUTS
 --NVL(INTEREST_DMD,0) - NVL(INTEREST_PAID,0)  INTEREST_OUTS
 --NVL(PENAL_INT_DMD,0) - NVL(PENAL_INT_PAID,0) PENAL_INT_OUTS
 FROM
 (
 SELECT
 SUM(NVL(PRINCIPAL_DMD,0)) PRINCIPAL_DMD,
 SUM(NVL(INTEREST_DMD,0)) INTEREST_DMD,
 SUM(NVL(PENAL_INT_DMD,0)) PENAL_INT_DMD
 FROM
 (
 SELECT
 CASE WHEN DMD_FLOW_ID = 'PRDEM' THEN DMD_AMT
 END PRINCIPAL_DMD,
 CASE WHEN DMD_FLOW_ID = 'INDEM' THEN DMD_AMT
 END INTEREST_DMD,
 CASE WHEN DMD_FLOW_ID = 'PIDEM' THEN DMD_AMT
 END PENAL_INT_DMD
 FROM TBAADM.LDT
 WHERE DMD_EFF_DATE <= (select db_stat_date-1 from tbaadm.gct )
 AND ACID = GAM.ACID
 AND DEL_FLG = 'N'
 AND BANK_ID = '01'
 )
 ),
 (
 SELECT
 SUM(NVL(PRINCIPAL_PAID,0)) PRINCIPAL_PAID,
 SUM(NVL(INTEREST_PAID,0)) INTEREST_PAID,
 SUM(NVL(PENAL_INT_PAID,0)) PENAL_INT_PAID
 FROM
 (
 SELECT ACID,
 CASE WHEN DMD_FLOW_ID = 'PRDEM' THEN ADJ_AMT
 END PRINCIPAL_PAID,
 CASE WHEN DMD_FLOW_ID = 'INDEM' THEN ADJ_AMT
 END INTEREST_PAID,
 CASE WHEN DMD_FLOW_ID = 'PIDEM' THEN ADJ_AMT
 END PENAL_INT_PAID
 FROM TBAADM.LDA
 WHERE TRAN_DATE <= (select db_stat_date-1 from tbaadm.gct )
 AND ACID = GAM.ACID
 AND BANK_ID = '01'
 )
 )
 )
PRINCIPAL_OUTS,

(SELECT
-- NVL(PRINCIPAL_DMD,0) - NVL(PRINCIPAL_PAID,0) PRINCIPAL_OUTS,
 NVL(INTEREST_DMD,0) - NVL(INTEREST_PAID,0)  INTEREST_OUTS
 --NVL(PENAL_INT_DMD,0) - NVL(PENAL_INT_PAID,0) PENAL_INT_OUTS
 FROM
 (
 SELECT
 SUM(NVL(PRINCIPAL_DMD,0)) PRINCIPAL_DMD,
 SUM(NVL(INTEREST_DMD,0)) INTEREST_DMD,
 SUM(NVL(PENAL_INT_DMD,0)) PENAL_INT_DMD
 FROM
 (
 SELECT
 CASE WHEN DMD_FLOW_ID = 'PRDEM' THEN DMD_AMT
 END PRINCIPAL_DMD,
 CASE WHEN DMD_FLOW_ID = 'INDEM' THEN DMD_AMT
 END INTEREST_DMD,
 CASE WHEN DMD_FLOW_ID = 'PIDEM' THEN DMD_AMT
 END PENAL_INT_DMD
 FROM TBAADM.LDT
 WHERE DMD_EFF_DATE <= (select db_stat_date-1 from tbaadm.gct )
 AND ACID = GAM.ACID
 AND DEL_FLG = 'N'
 AND BANK_ID = '01'
 )
 ),
 (
 SELECT
 SUM(NVL(PRINCIPAL_PAID,0)) PRINCIPAL_PAID,
 SUM(NVL(INTEREST_PAID,0)) INTEREST_PAID,
 SUM(NVL(PENAL_INT_PAID,0)) PENAL_INT_PAID
 FROM
 (
 SELECT ACID,
 CASE WHEN DMD_FLOW_ID = 'PRDEM' THEN ADJ_AMT
 END PRINCIPAL_PAID,
 CASE WHEN DMD_FLOW_ID = 'INDEM' THEN ADJ_AMT
 END INTEREST_PAID,
 CASE WHEN DMD_FLOW_ID = 'PIDEM' THEN ADJ_AMT
 END PENAL_INT_PAID
 FROM TBAADM.LDA
 WHERE TRAN_DATE <= (select db_stat_date-1 from tbaadm.gct )
 AND ACID = GAM.ACID
 AND BANK_ID = '01'
 )
 )
 )
INTEREST_OVERDUES,

(SELECT
-- NVL(PRINCIPAL_DMD,0) - NVL(PRINCIPAL_PAID,0) PRINCIPAL_OUTS,
 --NVL(INTEREST_DMD,0) - NVL(INTEREST_PAID,0)  INTEREST_OUTS
 NVL(PENAL_INT_DMD,0) - NVL(PENAL_INT_PAID,0) PENAL_INT_OUTS
 FROM
 (
 SELECT
 SUM(NVL(PRINCIPAL_DMD,0)) PRINCIPAL_DMD,
 SUM(NVL(INTEREST_DMD,0)) INTEREST_DMD,
 SUM(NVL(PENAL_INT_DMD,0)) PENAL_INT_DMD
 FROM
 (
 SELECT
 CASE WHEN DMD_FLOW_ID = 'PRDEM' THEN DMD_AMT
 END PRINCIPAL_DMD,
 CASE WHEN DMD_FLOW_ID = 'INDEM' THEN DMD_AMT
 END INTEREST_DMD,
 CASE WHEN DMD_FLOW_ID = 'PIDEM' THEN DMD_AMT
 END PENAL_INT_DMD
 FROM TBAADM.LDT
 WHERE DMD_EFF_DATE <= (select db_stat_date-1 from tbaadm.gct )
 AND ACID = GAM.ACID
 AND DEL_FLG = 'N'
 AND BANK_ID = '01'
 )
 ),
 (
 SELECT
 SUM(NVL(PRINCIPAL_PAID,0)) PRINCIPAL_PAID,
 SUM(NVL(INTEREST_PAID,0)) INTEREST_PAID,
 SUM(NVL(PENAL_INT_PAID,0)) PENAL_INT_PAID
 FROM
 (
 SELECT ACID,
 CASE WHEN DMD_FLOW_ID = 'PRDEM' THEN ADJ_AMT
 END PRINCIPAL_PAID,
 CASE WHEN DMD_FLOW_ID = 'INDEM' THEN ADJ_AMT
 END INTEREST_PAID,
 CASE WHEN DMD_FLOW_ID = 'PIDEM' THEN ADJ_AMT
 END PENAL_INT_PAID
 FROM TBAADM.LDA
 WHERE TRAN_DATE <= (select db_stat_date-1 from tbaadm.gct )
 AND ACID = GAM.ACID
 AND BANK_ID = '01'
 )
 )
 )
PENAL_INT_OUTS,

TRUNC(NVL((SELECT SUM(DMD_AMT - TOT_ADJ_AMT)
FROM TBAADM.LDT
WHERE DMD_OVDU_DATE > (select db_stat_date-1 from tbaadm.gct )
and DMD_EFF_DATE <=(select db_stat_date-1 from tbaadm.gct )
 AND DMD_AMT - TOT_ADJ_AMT != 0
 AND DMD_FLOW_ID IN ('INDEM')
 AND DEL_FLG = 'N'
 AND LDT.ACID = GAM.ACID), 0), 2) AS INDEM_DEFFERED_AMT,

 TRUNC(NVL((SELECT SUM(DMD_AMT - TOT_ADJ_AMT)
FROM TBAADM.LDT
WHERE DMD_OVDU_DATE > (select db_stat_date-1 from tbaadm.gct )
and DMD_EFF_DATE <=(select db_stat_date-1 from tbaadm.gct )
 AND DMD_AMT - TOT_ADJ_AMT != 0
 AND DMD_FLOW_ID IN ('PIDEM')
 AND DEL_FLG = 'N'
 AND LDT.ACID = GAM.ACID), 0), 2) AS PIDEM_DEFFERED_AMT,

  TRUNC(NVL((SELECT SUM(DMD_AMT - TOT_ADJ_AMT)
FROM TBAADM.LDT
WHERE DMD_OVDU_DATE > (select db_stat_date-1 from tbaadm.gct )
and DMD_EFF_DATE <=(select db_stat_date-1 from tbaadm.gct )
 AND DMD_AMT - TOT_ADJ_AMT != 0
 AND DMD_FLOW_ID IN ('PRDEM')
 AND DEL_FLG = 'N'
 AND LDT.ACID = GAM.ACID), 0), 2) AS PRDEM_DEFFERED_AMT
, Corp.corp_key FinacleCptyCIFId
,corp.corporate_name FinacleCptyName,
--,UPPER(custom.all_report_functions.get_crm_clang_desc(corp.cust_const,'CONSTITUTION_CODE'))legalentity_type

CASE 
    WHEN UPPER(custom.all_report_functions.get_crm_clang_desc(corp.cust_const,'CONSTITUTION_CODE')) = 'MINISTRY' THEN 'SOVEREIGN'
    WHEN UPPER(custom.all_report_functions.get_crm_clang_desc(corp.cust_const,'CONSTITUTION_CODE')) = 'LIMITED LIABILITY COMPANY' THEN 'NON-FINANCIAL CORPORATE'
    WHEN UPPER(custom.all_report_functions.get_crm_clang_desc(corp.cust_const,'CONSTITUTION_CODE')) = 'PRIVATE COMPANY LIMITED BY SHARES (LTD)' THEN 'NON-FINANCIAL CORPORATE'
    WHEN UPPER(custom.all_report_functions.get_crm_clang_desc(corp.cust_const,'CONSTITUTION_CODE')) = 'PARASTATAL' THEN 'PUBLIC SECTOR ENTITY'
    WHEN UPPER(custom.all_report_functions.get_crm_clang_desc(corp.cust_const,'CONSTITUTION_CODE')) = 'PRIVATE SECTOR ENTITY' THEN 'NON-FINANCIAL CORPORATE'
    WHEN UPPER(custom.all_report_functions.get_crm_clang_desc(corp.cust_const,'CONSTITUTION_CODE')) = 'MULTILATERAL' THEN 'MULTILATERAL DEVELOPMENT BANK'
    WHEN UPPER(custom.all_report_functions.get_crm_clang_desc(corp.cust_const,'CONSTITUTION_CODE')) = 'NON-GOVERNMENTAL FINANCIAL INSTITUTION' THEN 'FINANCIAL INSTITUTION'
    WHEN UPPER(custom.all_report_functions.get_crm_clang_desc(corp.cust_const,'CONSTITUTION_CODE')) = 'PUBLIC LIMITED COMPANY (PLC)' THEN 'NON-FINANCIAL CORPORATE'
    WHEN UPPER(custom.all_report_functions.get_crm_clang_desc(corp.cust_const,'CONSTITUTION_CODE')) = 'SOLE PROPRIETORSHIP' THEN 'NON-FINANCIAL CORPORATE'
    WHEN UPPER(custom.all_report_functions.get_crm_clang_desc(corp.cust_const,'CONSTITUTION_CODE')) = 'TRUST' THEN 'NON-FINANCIAL CORPORATE'
    ELSE 'UNKNOWN' -- Default value for unmatched cases
END AS FinacleCptyType

from tbaadm.gam,tbaadm.lam ,curr_Hol_Mapping c ,crmuser.corporate corp
where gam.acid =lam.acid
and gam.acct_CRNCY_CODE =c.CURRENY
and gam.cif_id =corp.corp_key
and gam.schm_code !='900'
 and gam.ENTITY_CRE_FLG ='Y'
 and PAYOFF_FLG !='Y'
--and gam.foracid in ( '200001001300001' )
and  gam.acid not in (select acid from tbaadm.gam where ACCT_CLS_DATE <=(select db_stat_date-1 from tbaadm.gct ) and schm_type ='CLA')
and  gam.acid not in (select acid from tbaadm.cot )

))
SELECT CAST ( 'Action'||','||'ExternalRefId'||','||'TradeId'||','||'TradeCounterParty'||','||'TradeBook'||','||'TradeDateTime'||','||'TraderName'||','||'SalesPerson'||','||'Comment'||','||'ProductType'||','||'ProductSubType'||','||'DiscountMethod'||','||'LegType'||','||'PayRec'||','||'StartDate'||','||'EndDate'||','||'OpenTermB'||','||'BehavioralMaturity'||','||'NoticeDays'||','||'Currency'||','||'Amount'||','||'PrincipalExchangeInitialB'||','||'PrincipalExchangeFinalB'||','||'Rate'||','||'FloatingRateReset'||','||'RateIndex'||','||'RateIndexSource'||','||'Tenor'||','||'Spread'||','||'DayCountConvention'||','||'DateRollConvention'||','||'PaymentFrequency'||','||'DiscountMethod'||','||'CouponPaymentAtEndB'||','||'AccrualPeriodAdjustment'||','||'IncludeFirstB'||','||'IncludeLastB'||','||'RollDay'||','||'HolidayCode'||','||'SettlementHolidays'||','||'AmountsRounding'||','||'RatesRounding'||','||'RatesRoundingDecPlaces'||','||'IgnoreFullCouponForNotionalAmort'||','||'SecuredFundingB'||','||'StubPeriod'||','||'SpecificFirstDate'||','||'SpecificLastDate'||','||'CustomStubTolerance'||','||'InterestCompounding'||','||'InterestCompoundingMethod'||','||'CpnHoliday'||','||'InterestCompoundingFrequency'||','||'ResetLag'||','||'CapitalizeB'||','||'keyword.1_MM Classification'||','||'keyword.FinacleCptyCIFId'||','||'keyword.FinacleCptyName'||','||'keyword.FinacleCptyType'||','||'PrincipalStructure_AmortizationType'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount' as VARCHAR2(4000 CHAR))   data
from dual
   union
SELECT CAST (
'NEW'   ||','||foracid || event ||','||NULL ||','|| 'FINACLE_CBS_COUNTERPARTY' ||','||'CBS_LOANS_DEPOSITS' ||','||TO_CHAR(ACCT_OPN_DATE, 'YYYYMMDD') ||','||'Finacle' ||','|| 'Finacle' ||','||'Bullect Fixed' ||','||'StructuredFlows' ||','||null ||','|| null ||','||'FIXED' ||','||'PAY' ||','||TO_CHAR(ACCT_OPN_DATE, 'YYYYMMDD')||','||TO_CHAR(Enddate, 'YYYYMMDD')||','||NULL||','||NULL||','||NULL||','||acct_CRNCY_CODE||','||amount||','||'TRUE' ||','||'TRUE' ||','|| rate||','|| null ||','|| null ||','|| null  ||','|| null ||','||null ||','||'ACT/360' ||','||'MOD_FOLLOW' ||','|| 'ZC' ||','|| NULL ||','||NULL ||','||NULL ||','||NULL  ||','|| NULL ||','|| NULL ||','||
HOLIDAY_CALENDAR ||','||HOLIDAY_CALENDAR  ||','||'NEAREST' ||','|| 'NEAREST' ||','||8 ||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL ||','|| NULL ||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| 'Finacle_Liquidity'||','||FINACLECPTYCIFID ||','|| REPLACE(FINACLECPTYNAME, ',', '-')  ||','|| FINACLECPTYTYPE ||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL ||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL as VARCHAR2(4000 CHAR))   data

FROM (
  SELECT *
  FROM cte
)
UNPIVOT (
  amount FOR event IN (
     PRINCIPAL_OUTS AS '_PRDEM_OVERDUE',
     INTEREST_OVERDUES AS '_INDEM_OVERDUE',
    PENAL_INT_OUTS AS '_PIDEM_OVERDUE'
  )
) ORDER BY 1 