select 'Action'||','||'ExternalRefId'||','||'TradeId'||','||'TradeCounterParty'||','||'TradeBook'||','||'TradeDateTime'||','||'TraderName'||','||'SalesPerson'||','||'Comment'||','||'ProductType'||','||'ProductSubType'||','||'DiscountMethod'||','||'LegType'||','||'PayRec'||','||'StartDate'||','||'EndDate'||','||'OpenTermB'||','||'BehavioralMaturity'||','||'NoticeDays'||','||'Currency'||','||'Amount'||','||'PrincipalExchangeInitialB'||','||'PrincipalExchangeFinalB'||','||'Rate'||','||'FloatingRateReset'||','||'RateIndex'||','||'RateIndexSource'||','||'Tenor'||','||'Spread'||','||'DayCountConvention'||','||'DateRollConvention'||','||'PaymentFrequency'||','||'DiscountMethod'||','||'CouponPaymentAtEndB'||','||'AccrualPeriodAdjustment'||','||'IncludeFirstB'||','||'IncludeLastB'||','||'RollDay'||','||'HolidayCode'||','||'SettlementHolidays'||','||'AmountsRounding'||','||'RatesRounding'||','||'RatesRoundingDecPlaces'||','||'IgnoreFullCouponForNotionalAmort'||','||'SecuredFundingB'||','||'StubPeriod'||','||'SpecificFirstDate'||','||'SpecificLastDate'||','||'CustomStubTolerance'||','||'InterestCompounding'||','||'InterestCompoundingMethod'||','||'CpnHoliday'||','||'InterestCompoundingFrequency'||','||'ResetLag'||','||'CapitalizeB'||','||'keyword.1_MM Classification'||','||'PrincipalStructure_AmortizationType'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount'||','||'AmortizationSchedule_AmortizationDate'||','||'AmortizationSchedule_AmortizationAmount' data
from dual
   union
select 'NEW'  ||','||
FORACID ||','||null ||','||  REPLACE(ACCT_NAME, ',', '-') ||','||'CBS_LOANS_DEPOSITS' ||','||TO_CHAR(ACCT_OPN_DATE, 'YYYYMMDD') ||','||'Finacle' ||','|| 'Finacle' ||','||'Bullect Fixed' ||','||'StructuredFlows' ||','||null ||','|| null ||','||'FIXED' ||','||CASE WHEN CLR_BAL_AMT  > 0 then 'PAY' ELSE 'REC' END ||','||to_char(ACCT_OPN_DATE,'YYYYMMDD')||','||NULL||','||'TRUE'||','||'1M'||','||7||','||acct_CRNCY_CODE||','||ABS(CLR_BAL_AMT ) ||','||'TRUE' ||','||'TRUE' ||','|| rate||','|| null ||','|| null ||','|| null  ||','|| null ||','||null ||','||'ACT/360' ||','||'MOD_FOLLOW' ||','|| 'ZC' ||','|| NULL ||','||NULL ||','||NULL ||','||NULL  ||','|| NULL ||','|| NULL ||','||
HOLIDAY_CALENDAR ||','||HOLIDAY_CALENDAR  ||','||'NEAREST' ||','|| 'NEAREST' ||','||8 ||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL ||','|| NULL ||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| 'Finacle_Liquidity'||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL ||','|| NULL||','|| NULL||','|| NULL||','|| NULL||','|| NULL DATA
from (
select foracid,acct_name,Clr_bal_amt,c.HOLIDAY_CALENDAR,
schm_code,
gam.sol_id,
ACCT_OPN_DATE,acct_CRNCY_CODE ,
(select nvl((select PREF_RATE_PCNT from tbaadm.ITCP where ENTITY_ID=gam.acid and ENTITY_TYPE='ACCNT'
and PREF_TYPE='ACCT' and  PREF_SUB_TYPE='C'
and INT_PREF_SRL_NUM in  (select max(INT_PREF_SRL_NUM) from tbaadm.ITCP where ENTITY_ID=gam.acid and ENTITY_TYPE='ACCNT'
and PREF_TYPE='ACCT' and  PREF_SUB_TYPE='C')
and rownum=1),0) +
nvl((select PREF_RATE_PCNT from tbaadm.ITCP where ENTITY_ID=gam.acid and ENTITY_TYPE='ACCNT'
and PREF_TYPE='CUST' and  PREF_SUB_TYPE='C'
and INT_PREF_SRL_NUM in  (select max(INT_PREF_SRL_NUM) from tbaadm.ITCP where ENTITY_ID=gam.acid and ENTITY_TYPE='ACCNT'
and PREF_TYPE='CUST' and  PREF_SUB_TYPE='C')
and rownum=1),0)
from dual)
RATE,
acct_CRNCY_CODE Currency
from tbaadm.gam
left join curr_Hol_Mapping c on gam.acct_CRNCY_CODE =c.CURRENY
where
 gam.ACCT_CLS_FLG='N'
and gam.entity_cre_flg='Y'
and schm_code in ('200','220')
and Clr_bal_amt !=0
--and gam.foracid in ('500002101200001')
and schm_type ='CAA'
and ACCT_OPN_DATE <= (select db_stat_date-1 from tbaadm.gct)
)order by 1 




