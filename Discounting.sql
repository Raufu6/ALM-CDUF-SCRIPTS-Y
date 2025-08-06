select 'Action'||','||'ExternalRefId'||','||'TradeId'||','||'TradeCounterParty'||','||'TradeBook'||','||'TradeDateTime'||','||'TraderName'||','||'SalesPerson'||','||'Comment'||','||'ProductType'||','||'ProductSubType'||','||'DiscountMethod'||','||'LegType'||','||'PayRec'||','||'StartDate'||','||'EndDate'||','||'OpenTermB'||','||'BehavioralMaturity'||','||'NoticeDays'||','||'Currency'||','||'Amount'||','||'PrincipalExchangeInitialB'||','||'PrincipalExchangeFinalB'||','||'Rate'||','||'FloatingRateReset'||','||'RateIndex'||','||'RateIndexSource'||','||'Tenor'||','||'Speard'||','||'DayCountConvention'||','||'DateRollConvention'||','||'PaymentFrequency'||','||'CouponPaymentAtEndB'||','||'AccrualPeriodAdjustment'||','||'IncludeFirstB'||','||'IncludeLastB'||','||'RollDay'||','||'HolidayCode'||','||'SettlementHolidays'||','||'AmountsRounding'||','||'RatesRounding'||','||'RatesRoundingDecPlaces'||','||'IgnoreFullCouponForNotionalAmort'||','||'SecuredFundingB'||','||'StubPeriod'||','||'SpecificFirstDate'||','||'SpecificLastDate'||','||'CustomStubTolerance'||','||'InterestCompounding'||','||'InterestCompoundingMethod'||','||'CpnHoliday'||','||'InterestCompoundingFrequency'||','||'ResetLag'||','||'CapitalizeB'||','||'keyword.1_MM Classification' ||','||'PrincipalStructure_AmortizationType'
from dual
union
select 'NEW' || ',' || rec.BILL_ID || ',' || '' || ',' ||  REPLACE('acct_name', ',', '-') || ',' || 'CBS_LOANS_DEPOSITS' || ',' || TO_CHAR(rec.db_stat_date, 'YYYYMMDD') || ',' || 'Finacle' || ',' || 'Finacle' || ',' ||CASE
    WHEN rec.discounting_boolen = 'Y'
         AND rec.compound_boolen = 'Y'
         AND rec.amortization_boolen = 'Y'
         AND rec.float_boolen = 'Y' THEN 'BillDiscountAmortizationCompoundingFloat'
    WHEN rec.discounting_boolen = 'Y'
         AND rec.compound_boolen = 'Y'
         AND rec.bullet_boolen = 'Y'
         AND rec.float_boolen = 'Y' THEN 'BillDiscountBulletCompoundingFloat'
    WHEN rec.discounting_boolen = 'Y'
         AND rec.bullet_boolen = 'Y'
         AND rec.fixed_boolen = 'Y' THEN 'BillDiscountBulletFixed'
    WHEN rec.discounting_boolen = 'Y'
         AND rec.bullet_boolen = 'Y'
         AND rec.float_boolen = 'Y' THEN 'BillDiscountBulletFloat'

    ELSE NULL END
 || ',' || 'StructuredFlows' || ',' || 'Discount' || ',' || 'PREPAID' || ',' || 'FIXED' || ',' || 'REC' || ',' || to_char(rec.VFD_BOD_DATE, 'YYYYMMDD') || ',' || to_char(rec.maturity, 'YYYYMMDD') || ',' || '' || ',' || '' || ',' || '' || ',' || CURRENCY || ',' || abs(rec.Loan_balance) || ',' || 'TRUE' || ',' || 'TRUE' || ',' || rec.INTEREST_RATE || ',' || '' || ',' || '' || ',' || '' || ',' || '' || ',' || '' || ',' || 'ACT/360' || ',' || 'MOD_FOLLOW' ||',' ||'ZC'|| ',' ||'FALSE'|| ',' || '' || ',' || '' || ',' || '' || ',' || '' || ',' || '' || ',' || '' || ',' || 'NEAREST' || ',' || 'NEAREST' || ',' || '' || ',' || '' || ',' || '' || ',' || '' || ',' || '' || ',' || '' || ',' || '' || ',' || '' || ',' || '' || ',' || '' || ',' || '' || ',' || '' || ',' ||'' || ','  || 'Finacle_Liquidity' || ',' || 'Bullet'
from (
SELECT
'Discount' ProductSubType,
'PREPAID' DiscountMethod,
'B' INTEREST_LR_FREQ_TYPE,
(select INT_DESC from tbaadm.IC_ITCM where INT_TBL_CODE =int_period_tbl_code)INT_DESC,
'N' compound_boolen,
'N' amortization_boolen,
'Y' discounting_boolen,
'N' overdue_boolen,
'N' float_boolen,
'Y' Fixed_boolen,
'Y' bullet_boolen,
sysdate system_date,
(select db_stat_date-1 from tbaadm.gct)db_stat_date,
F.bill_id ||'_' ||to_char(VFD_BOD_DATE,'ddmmyyyy')||'_'||TRAN_ID || '_'|| EVENT_NUM bill_id,
H.BILL_LIAB + H.DISC_INT_AMT Loan_balance,
abs(H.INTEREST_RATE)INTEREST_RATE,
TO_CHAR(i.peg_review_date,'MM/DD/YYYY') next_repricing_date,
int_period_tbl_code LIBOR,
(SELECT TO_CHAR(min(tran_date),'MM/DD/YYYY') FROM tbaadm.fae WHERE bill_id = f.bill_id AND part_tran_type = 'D' AND bill_func = 'P' AND acid = bp_acid )libor_start_date,
NRML_PCNT_DR as "LIBOR_PCNT",
i.id_dr_pref_pcnt nominal_margin,
( i.nrml_pcnt_dr + i.id_dr_pref_pcnt ) net_nominal_int,
null net_default_pcnt,
F.DUE_DATE maturity,
F.DUE_DATE next_installment_date,
F.BILL_CRNCY_CODE currency,
H.VFD_BOD_DATE,
m.acct_name

from CUSTOM.FX_BILL_MASTER_HISTORY_TABLE F,
custom.c_EIT E,
TBAADM.FBH H,
TBAADM.ITC I,
tbaadm.gam M
where
TO_DATE(F.DB_STAT_DATE) =(select db_stat_date -1 from tbaadm.gct)
and TO_DATE(e.DB_STAT_DATE) =(select db_stat_date -1 from tbaadm.gct)
AND   F.BILL_B2K_ID = E.entity_id
AND   E.entity_id   = I.entity_id
and F.BILL_ID       = H.BILL_ID
AND I.ENTITY_ID     = F.BILL_B2K_ID
and M.acid =BP_ACID
AND I.ENTITY_TYPE = 'FBILL'
AND H.BILL_FUNC = 'P'
AND H.ENTITY_CRE_FLG = 'Y'
AND H.DEL_FLG = 'N'
AND M.ENTITY_CRE_FLG = 'Y'
AND F.ENTITY_CRE_FLG = 'Y'
AND F.DEL_FLG = 'N'
--and F.bill_id ='BEDU3021090001'
AND I.INT_TBL_CODE_SRL_NUM IN (SELECT MAX(INT_TBL_CODE_SRL_NUM) FROM TBAADM.ITC T WHERE T.ENTITY_ID = I.ENTITY_ID
AND T.ENTITY_TYPE = 'FBILL')) rec
ORDER BY 1;

