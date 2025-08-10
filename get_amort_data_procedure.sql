CREATE OR REPLACE FUNCTION CUSTOM.get_amort_data RETURN CLOB IS

    v_system_date                 CLOB := '';
    v_db_stat_date                CLOB := '';
    v_parent_limit                CLOB := '';
    v_acct_crncy_code             CLOB := '';
    v_account_no                  CLOB := '';
    v_interest_demand             CLOB := '';
    v_interest_demand_usd         CLOB := '';
    v_default_interest_demand     CLOB := '';
    v_default_interest_demand_usd CLOB := '';
    v_principal_demand            CLOB := '';
    v_principal_demand_usd        CLOB := '';
    v_demand_date                 CLOB := '';
    v_overdue_date                CLOB := '';
    v_clr_bal_amt                 CLOB := '';
    v_dmd_eff_date                CLOB := '';
    v_dmd_ovdu_date               CLOB := '';
    v_action                      VARCHAR2(100) := 'NEW';
    v_comment                     VARCHAR2(100) := '';
    v_running_balance             NUMBER(20, 4);
    v_header                 CLOB := 'Action,ExternalRefId,TradeId,TradeCounterParty,TradeBook,TradeDateTime,TraderName,SalesPerson,Comment,ProductType,LegType,PayRec,StartDate,EndDate,OpenTermB,BehavioralMaturity,NoticeDays,Currency,Amount,PrincipalExchangeInitialB,PrincipalExchangeFinalB,Rate,FloatingRateReset,RateIndex,RateIndexSource,Tenor,Spread,DayCountConvention,DateRollConvention,PaymentFrequency,DiscountMethod,CouponPaymentAtEndB,AccrualPeriodAdjustment,IncludeFirstB,IncludeLastB,RollDay,HolidayCode,SettlementHolidays,AmountsRounding,RatesRounding,RatesRoundingDecPlaces,IgnoreFullCouponForNotionalAmort,SecuredFundingB,StubPeriod,SpecificFirstDate,SpecificLastDate,CustomStubTolerance,InterestCompounding,InterestCompoundingMethod,CpnHoliday,InterestCompoundingFrequency,ResetLag,CapitalizeB,keyword.1_MM Classification,keyword.FinacleCptyCIFId,keyword.FinacleCptyName,keyword.FinacleCptyType,PrincipalStructure_AmortizationType';
    v_header2   CLOB := ',AmortizationSchedule_AmortizationDate,AmortizationSchedule_AmortizationAmount';-- || chr(10)
    v_header3   CLOB := '';
    result_output               CLOB := '';
    v_running_balance_s           CLOB := '' ;
    v_acct_name                   VARCHAR2(100) := '';
    v_leg_type                    VARCHAR2(100) := '';
    v_output              CLOB := '';
    v_idx                 NUMBER(20, 4) := 1;
     v_date_part           VARCHAR2(100);
    v_balance_part        VARCHAR2(100);
    v_start_date            VARCHAR2(100);
    V_end_date               VARCHAR2(100);
    v_prev_date_part               VARCHAR2(100);
    V_counter   NUMBER(20, 4) := 1;

    CURSOR demand_cursor (
        c_foracid VARCHAR2
    ) IS
    SELECT
         system_date,
        db_stat_date,
        parent_limit,
        acct_crncy_code,
        account_no,
        interest_demand,
        interest_demand_usd,
        default_interest_demand,
        default_interest_demand_usd,
        principal_demand,
        acct_name,
        principal_demand_usd,
        demand_date,
        overdue_date,
        clr_bal_amt,
        dmd_eff_date,
        dmd_ovdu_date,
        holiday_calendar
    FROM
       custom.GETAMORT_DEMAND
            WHERE trim(account_no) = trim(c_foracid)
    ORDER BY
        demand_date;

    CURSOR loan_cursor IS
     
     SELECT
        (
            SELECT
                decode(lrs2.lr_freq_type, 'Q', 'Quaterly', 'H', 'Half yearly',
                       'M', 'Monthly', 'B', 'Bullet', 'D',
                       'Daily', 'Y', 'Yearly')
            FROM
                tbaadm.lrs lrs2
            WHERE
                    lrs2.flow_id = 'INDEM'
                AND lrs2.entity_cre_flg = 'Y'
                AND lrs2.acid = gam.acid
        )                                                                  interest_lr_freq_type,
        (
            SELECT
                int_desc
            FROM
                tbaadm.ic_itcm
            WHERE
                int_tbl_code = itc.int_tbl_code
        )                                                                  AS int_desc,
        CASE
            WHEN schm_code IN ( '411', '431', '401', '421', '403',
                                '492', '493' ) THEN
                'Y'
            ELSE
                'N'
        END                                                                AS compound_boolen,
        (
            CASE
                WHEN ( (
                    SELECT
                        COUNT(1)
                    FROM
                        custom.c_amort
                    WHERE
                            c_amort.foracid = gam.foracid
                        AND flow_date >= (
                            SELECT
                                db_stat_date - 1
                            FROM
                                tbaadm.gct
                        )
                        AND principle_amt != 0
                ) > 0 ) THEN
                    'Y'
                ELSE
                    'N'
            END
        )                                                                  amortization_boolen,
        CASE
            WHEN schm_code IN ( '305', '306' ) THEN
                'Y'
            ELSE
                'N'
        END                                                                AS discounting_boolen,
        CASE
            WHEN nvl((
                SELECT
                    SUM(ldt.dmd_amt - ldt.tot_adj_amt)
                FROM
                    tbaadm.ldt
                WHERE
                        ldt.dmd_amt - ldt.tot_adj_amt != 0
                    AND ldt.del_flg = 'N'
                    AND ldt.acid = gam.acid
            ),
                     0) != 0 THEN
                'Y'
            ELSE
                'N'
        END                                                                AS overdue_boolen,
        CASE
            WHEN itc.int_tbl_code != 'CLZER' THEN
                'Y'
            ELSE
                'N'
        END                                                                AS float_boolen,
        CASE
            WHEN itc.int_tbl_code = 'CLZER' THEN
                'Y'
            ELSE
                'N'
        END                                                                AS fixed_boolen,
        'N'                                                                AS bullet_boolen,
        sysdate                                                            AS system_date,
        (
            SELECT
                db_stat_date - 1
            FROM
                tbaadm.gct
        )                                                                  AS db_stat_date,
        rep_shdl_date,
        dis_shdl_date,
        ei_perd_start_date                                                 AS account_start_date,
        ei_perd_end_date                                                   AS account_end_date,
        dis_amt                                                            AS disbursment_amt,
        foracid,
        schm_code,
        clr_bal_amt                                                        AS account_balance,
        flow_id,
        flow_start_date,
        lr_freq_type                                                       AS lr_freq_type_code,
        decode(lr_freq_type, 'Q', 'Quaterly', 'H', 'Half yearly',
               'M', 'Monthly', 'B', 'Bullet', 'D',
               'Daily', 'Y', 'Yearly')                                     AS lr_freq_type_desc,
        num_of_flows,
        num_of_dmds,
        flow_amt,
        next_dmd_date,
        acct_opn_date,
        decode(lr_freq_hldy_stat, 'N', 'Next day', 'P', 'Previous day')    AS lr_freq_hldy_stat,
        decode(itc.int_tbl_code, 'CLZER', 'Fixed', 'Floating')             AS repricing_plan,
        itc.int_tbl_code                                                   AS base_rate,
        int_period_tbl_code,
        itc.cust_dr_pref_pcnt                                              AS cas,
        itc.nrml_pcnt_dr                                                   AS nominal_margin,
        itc.id_dr_pref_pcnt                                                AS spread,
        ( itc.nrml_pcnt_dr + itc.id_dr_pref_pcnt + itc.cust_dr_pref_pcnt ) AS net_nominal_int,
        p.penal_pref_pcnt                                                  AS net_default_pcnt,
        itc.start_date                                                     AS libor_start_date,
        itc.peg_review_date                                                AS next_repricing_date,
        tran_date_bal,
        nvl((
            SELECT
                SUM(ldt.dmd_amt - ldt.tot_adj_amt)
            FROM
                tbaadm.ldt
            WHERE
                    ldt.dmd_amt - ldt.tot_adj_amt != 0
                AND ldt.del_flg = 'N'
                AND ldt.acid = gam.acid
        ),
            0)                                                             AS overdue_boolen_amt,
        (
            SELECT
                c_rfr_param_tbl.look_back_period
            FROM
                custom.c_rfr_param_tbl
            WHERE
                    c_rfr_param_tbl.acid = gam.acid
                AND c_rfr_param_tbl.entity_cre_flg = 'Y'
                AND c_rfr_param_tbl.del_flg = 'N'
        )                                                                  AS look_back_period,
        (
            SELECT
                COUNT(1)
            FROM
                custom.c_amort
            WHERE
                    c_amort.foracid = gam.foracid
                AND flow_date >= (
                    SELECT
                        db_stat_date - 1
                    FROM
                        tbaadm.gct
                )
        )                                                                  c_amort_count,
        rateindex,
        rateindexsource,
        Tenor
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
    FROM
             tbaadm.lrs
        JOIN tbaadm.lam ON lam.acid = lrs.acid
        JOIN tbaadm.gam ON gam.acid = lam.acid
        JOIN tbaadm.lrp ON lrp.acid = lam.acid
        JOIN tbaadm.itc ON gam.acid = itc.entity_id
        JOIN tbaadm.itc_pnl    p ON p.entity_id = itc.entity_id
        JOIN tbaadm.eab ON gam.acid = eab.acid
        join crmuser.corporate corp on gam.cif_id =corp.corp_key
        LEFT JOIN rateindex_mapping r ON gam.acct_crncy_code = r.currency
                                         AND itc.int_tbl_code = r.BASE_RATE
    WHERE
        (
            SELECT
                db_stat_date - 1
            FROM
                tbaadm.gct
        ) BETWEEN eod_date AND end_eod_date
        AND itc.int_tbl_code_srl_num = (
            SELECT
                MAX(int_tbl_code_srl_num)
            FROM
                tbaadm.itc t
            WHERE
                t.entity_id = itc.entity_id
        )
        AND p.int_tbl_code_srl_num = (
            SELECT
                MAX(int_tbl_code_srl_num)
            FROM
                tbaadm.itc_pnl p1
            WHERE
                p1.entity_id = itc.entity_id
        )
        AND lrs.entity_cre_flg = 'Y'
        AND gam.entity_cre_flg = 'Y'
        AND gam.acct_cls_flg = 'N'
        AND gam.clr_bal_amt != 0
        ---AND gam.foracid IN ( '600084202410002' )
        AND lrs.flow_id = 'PRDEM'
        AND lrs.last_rec_flg = 'Y'
        AND gam.schm_code != '900';
             --AND ROWNUM <= 100;

   -- Record type to hold one row
    rec                           loan_cursor%rowtype;
    demand_rec                    demand_cursor%rowtype;
BEGIN
   -- Open cursor
    OPEN loan_cursor;
    LOOP
        FETCH loan_cursor INTO rec;
        EXIT WHEN loan_cursor%notfound;
        v_running_balance := abs(rec.account_balance);
        ---dbms_output.put_line(rec.amortization_boolen);
        IF ( rec.amortization_boolen = 'Y' ) THEN
        
        --dbms_output.put_line('After rec.amortization_boolen' || rec.foracid ||'X'||'  '|| length(rec.foracid));
            BEGIN
             --dbms_output.put_line('begin ');
                OPEN demand_cursor(rec.foracid);
                --dbms_output.put_line('open ');
                LOOP
               -- dbms_output.put_line('loop ');
                    FETCH demand_cursor INTO demand_rec;
                    --dbms_output.put_line(demand_cursor%rowcount);
                    EXIT WHEN demand_cursor%notfound;
                    
                    IF (demand_cursor%rowcount > 0)   THEN

                   -- dbms_output.put_line('Print Here');
                        v_system_date :=
                            CASE
                                WHEN v_system_date = '' THEN
                                    to_char(demand_rec.system_date, 'DD-MON-YYYY HH24:MI:SS')
                                ELSE v_system_date
                                     || '|'
                                     || to_char(demand_rec.system_date, 'DD-MON-YYYY HH24:MI:SS')
                            END;

                        v_db_stat_date := to_char(demand_rec.db_stat_date, 'YYYYMMDD');
                        v_parent_limit :=
                            CASE
                                WHEN v_parent_limit = '' THEN
                                    nvl(demand_rec.parent_limit, '')
                                ELSE v_parent_limit
                                     || '|'
                                     || nvl(demand_rec.parent_limit, '')
                            END;

                        v_acct_crncy_code := demand_rec.acct_crncy_code;
                        v_account_no :=
                            CASE
                                WHEN v_account_no = '' THEN
                                    nvl(demand_rec.account_no, '')
                                ELSE v_account_no
                                     || '|'
                                     || nvl(demand_rec.account_no, '')
                            END;

                        v_interest_demand :=
                            CASE
                                WHEN v_interest_demand = '' THEN
                                    to_char(nvl(demand_rec.interest_demand, 0))
                                ELSE v_interest_demand
                                     || '|'
                                     || to_char(nvl(demand_rec.interest_demand, 0))
                            END;

                        v_interest_demand_usd :=
                            CASE
                                WHEN v_interest_demand_usd = '' THEN
                                    to_char(nvl(demand_rec.interest_demand_usd, 0))
                                ELSE v_interest_demand_usd
                                     || '|'
                                     || to_char(nvl(demand_rec.interest_demand_usd, 0))
                            END;

                        v_default_interest_demand :=
                            CASE
                                WHEN v_default_interest_demand = '' THEN
                                    to_char(nvl(demand_rec.default_interest_demand, 0))
                                ELSE v_default_interest_demand
                                     || '|'
                                     || to_char(nvl(demand_rec.default_interest_demand, 0))
                            END;

                        v_default_interest_demand_usd :=
                            CASE
                                WHEN v_default_interest_demand_usd = '' THEN
                                    to_char(nvl(demand_rec.default_interest_demand_usd, 0))
                                ELSE v_default_interest_demand_usd
                                     || '|'
                                     || to_char(nvl(demand_rec.default_interest_demand_usd, 0))
                            END;

                        v_principal_demand :=
                            CASE
                                WHEN v_principal_demand = '' THEN
                                    to_char(nvl(demand_rec.principal_demand, 0))
                                ELSE v_principal_demand
                                     || '|'
                                     || to_char(nvl(demand_rec.principal_demand, 0))
                            END;

                        v_principal_demand_usd :=
                            CASE
                                WHEN v_principal_demand_usd = '' THEN
                                    to_char(nvl(demand_rec.principal_demand_usd, 0))
                                ELSE v_principal_demand_usd
                                     || '|'
                                     || to_char(nvl(demand_rec.principal_demand_usd, 0))
                            END;

                        v_demand_date :=
                            CASE
                                WHEN v_demand_date = '' THEN
                                    nvl(
                                        to_char(demand_rec.demand_date, 'YYYYMMDD'),
                                        ''
                                    )
                                ELSE v_demand_date
                                     || '|'
                                     || nvl(
                                    to_char(demand_rec.demand_date, 'YYYYMMDD'),
                                    ''
                                )
                            END;

                        v_overdue_date :=
                            CASE
                                WHEN v_overdue_date = '' THEN
                                    nvl(
                                        to_char(demand_rec.overdue_date, 'DD-MON-YYYY'),
                                        ''
                                    )
                                ELSE v_overdue_date
                                     || '|'
                                     || nvl(
                                    to_char(demand_rec.overdue_date, 'DD-MON-YYYY'),
                                    ''
                                )
                            END;

                        v_running_balance := v_running_balance - demand_rec.principal_demand;
                        v_running_balance_s :=
                            CASE
                                WHEN v_running_balance_s IS NULL
                                     OR v_running_balance_s = '' THEN
                                    to_char(nvl(v_running_balance, 0))
                                ELSE v_running_balance_s
                                     || '|'
                                     || to_char(nvl(v_running_balance, 0))
                            END;

                        v_clr_bal_amt :=
                            CASE
                                WHEN v_clr_bal_amt = '' THEN
                                    to_char(nvl(demand_rec.clr_bal_amt, 0))
                                ELSE v_clr_bal_amt
                                     || '|'
                                     || to_char(nvl(demand_rec.clr_bal_amt, 0))
                            END;

                        v_dmd_eff_date :=
                            CASE
                                WHEN v_dmd_eff_date = '' THEN
                                    nvl(
                                        to_char(demand_rec.dmd_eff_date, 'DD-MON-YYYY'),
                                        ''
                                    )
                                ELSE v_dmd_eff_date
                                     || '|'
                                     || nvl(
                                    to_char(demand_rec.dmd_eff_date, 'DD-MON-YYYY'),
                                    ''
                                )
                            END;

                        v_dmd_ovdu_date :=
                            CASE
                                WHEN v_dmd_ovdu_date = '' THEN
                                    nvl(
                                        to_char(demand_rec.dmd_ovdu_date, 'DD-MON-YYYY'),
                                        ''
                                    )
                                ELSE v_dmd_ovdu_date
                                     || '|'
                                     || nvl(
                                    to_char(demand_rec.dmd_ovdu_date, 'DD-MON-YYYY'),
                                    ''
                                )
                            END;

                    ELSE
                        v_system_date :=
                            CASE
                                WHEN v_system_date = '' THEN
                                    ''
                                ELSE v_system_date
                                     || '|'
                                     || ''
                            END;

                        v_db_stat_date :=
                            CASE
                                WHEN v_db_stat_date = '' THEN
                                    ''
                                ELSE v_db_stat_date
                                     || '|'
                                     || ''
                            END;

                        v_parent_limit :=
                            CASE
                                WHEN v_parent_limit = '' THEN
                                    ''
                                ELSE v_parent_limit
                                     || '|'
                                     || ''
                            END;

                        v_acct_crncy_code :=
                            CASE
                                WHEN v_acct_crncy_code = '' THEN
                                    ''
                                ELSE v_acct_crncy_code
                                     || '|'
                                     || ''
                            END;

                        v_account_no :=
                            CASE
                                WHEN v_account_no = '' THEN
                                    ''
                                ELSE v_account_no
                                     || '|'
                                     || ''
                            END;

                        v_interest_demand :=
                            CASE
                                WHEN v_interest_demand = '' THEN
                                    '0'
                                ELSE v_interest_demand
                                     || '|'
                                     || '0'
                            END;

                        v_interest_demand_usd :=
                            CASE
                                WHEN v_interest_demand_usd = '' THEN
                                    '0'
                                ELSE v_interest_demand_usd
                                     || '|'
                                     || '0'
                            END;

                        v_default_interest_demand :=
                            CASE
                                WHEN v_default_interest_demand = '' THEN
                                    '0'
                                ELSE v_default_interest_demand
                                     || '|'
                                     || '0'
                            END;

                        v_default_interest_demand_usd :=
                            CASE
                                WHEN v_default_interest_demand_usd = '' THEN
                                    '0'
                                ELSE v_default_interest_demand_usd
                                     || '|'
                                     || '0'
                            END;

                        v_principal_demand :=
                            CASE
                                WHEN v_principal_demand = '' THEN
                                    '0'
                                ELSE v_principal_demand
                                     || '|'
                                     || '0'
                            END;

                        v_principal_demand_usd :=
                            CASE
                                WHEN v_principal_demand_usd = '' THEN
                                    '0'
                                ELSE v_principal_demand_usd
                                     || '|'
                                     || '0'
                            END;

                        v_demand_date :=
                            CASE
                                WHEN v_demand_date = '' THEN
                                    ''
                                ELSE v_demand_date
                                     || '|'
                                     || ''
                            END;

                        v_overdue_date :=
                            CASE
                                WHEN v_overdue_date = '' THEN
                                    ''
                                ELSE v_overdue_date
                                     || '|'
                                     || ''
                            END;

                        v_clr_bal_amt :=
                            CASE
                                WHEN v_clr_bal_amt = '' THEN
                                    '0'
                                ELSE v_clr_bal_amt
                                     || '|'
                                     || '0'
                            END;

                        v_dmd_eff_date :=
                            CASE
                                WHEN v_dmd_eff_date = '' THEN
                                    ''
                                ELSE v_dmd_eff_date
                                     || '|'
                                     || ''
                            END;

                        v_dmd_ovdu_date :=
                            CASE
                                WHEN v_dmd_ovdu_date = '' THEN
                                    ''
                                ELSE v_dmd_ovdu_date
                                     || '|'
                                     || ''
                            END;

                    END IF;

                END LOOP;

                CLOSE demand_cursor;
            EXCEPTION
                WHEN OTHERS THEN
                    result_output := result_output
                                     || 'ERROR: '
                                     || substr(sqlerrm, 1, 200)
                                     || ' FORACID '
                                     || rec.foracid
                                     || chr(10);
                                     
                                     
                    dbms_output.put_line('ERROR ' || result_output);
                    v_system_date :=
                        CASE
                            WHEN v_system_date = '' THEN
                                ''
                            ELSE v_system_date
                                 || '|'
                                 || ''
                        END;

                    v_db_stat_date :=
                        CASE
                            WHEN v_db_stat_date = '' THEN
                                ''
                            ELSE v_db_stat_date
                                 || '|'
                                 || ''
                        END;

                    v_parent_limit :=
                        CASE
                            WHEN v_parent_limit = '' THEN
                                ''
                            ELSE v_parent_limit
                                 || '|'
                                 || ''
                        END;

                    v_acct_crncy_code :=
                        CASE
                            WHEN v_acct_crncy_code = '' THEN
                                ''
                            ELSE v_acct_crncy_code
                                 || '|'
                                 || ''
                        END;

                    v_account_no :=
                        CASE
                            WHEN v_account_no = '' THEN
                                ''
                            ELSE v_account_no
                                 || '|'
                                 || ''
                        END;

                    v_interest_demand :=
                        CASE
                            WHEN v_interest_demand = '' THEN
                                '0'
                            ELSE v_interest_demand
                                 || '|'
                                 || '0'
                        END;

                    v_interest_demand_usd :=
                        CASE
                            WHEN v_interest_demand_usd = '' THEN
                                '0'
                            ELSE v_interest_demand_usd
                                 || '|'
                                 || '0'
                        END;

                    v_default_interest_demand :=
                        CASE
                            WHEN v_default_interest_demand = '' THEN
                                '0'
                            ELSE v_default_interest_demand
                                 || '|'
                                 || '0'
                        END;

                    v_default_interest_demand_usd :=
                        CASE
                            WHEN v_default_interest_demand_usd = '' THEN
                                '0'
                            ELSE v_default_interest_demand_usd
                                 || '|'
                                 || '0'
                        END;

                    v_principal_demand :=
                        CASE
                            WHEN v_principal_demand = '' THEN
                                '0'
                            ELSE v_principal_demand
                                 || '|'
                                 || '0'
                        END;

                    v_principal_demand_usd :=
                        CASE
                            WHEN v_principal_demand_usd = '' THEN
                                '0'
                            ELSE v_principal_demand_usd
                                 || '|'
                                 || '0'
                        END;

                    v_demand_date :=
                        CASE
                            WHEN v_demand_date = '' THEN
                                ''
                            ELSE v_demand_date
                                 || '|'
                                 || ''
                        END;

                    v_overdue_date :=
                        CASE
                            WHEN v_overdue_date = '' THEN
                                ''
                            ELSE v_overdue_date
                                 || '|'
                                 || ''
                        END;

                    v_clr_bal_amt :=
                        CASE
                            WHEN v_clr_bal_amt = '' THEN
                                '0'
                            ELSE v_clr_bal_amt
                                 || '|'
                                 || '0'
                        END;

                    v_dmd_eff_date :=
                        CASE
                            WHEN v_dmd_eff_date = '' THEN
                                ''
                            ELSE v_dmd_eff_date
                                 || '|'
                                 || ''
                        END;

                    v_dmd_ovdu_date :=
                        CASE
                            WHEN v_dmd_ovdu_date = '' THEN
                                ''
                            ELSE v_dmd_ovdu_date
                                 || '|'
                                 || ''
                        END;

            END;
        ELSE
            v_system_date :=
                CASE
                    WHEN v_system_date = '' THEN
                        ''
                    ELSE v_system_date
                         || '|'
                         || ''
                END;

            v_db_stat_date :=
                CASE
                    WHEN v_db_stat_date = '' THEN
                        ''
                    ELSE v_db_stat_date
                         || '|'
                         || ''
                END;

            v_parent_limit :=
                CASE
                    WHEN v_parent_limit = '' THEN
                        ''
                    ELSE v_parent_limit
                         || '|'
                         || ''
                END;

            v_acct_crncy_code :=
                CASE
                    WHEN v_acct_crncy_code = '' THEN
                        ''
                    ELSE v_acct_crncy_code
                         || '|'
                         || ''
                END;

            v_account_no :=
                CASE
                    WHEN v_account_no = '' THEN
                        ''
                    ELSE v_account_no
                         || '|'
                         || ''
                END;

            v_interest_demand :=
                CASE
                    WHEN v_interest_demand = '' THEN
                        '0'
                    ELSE v_interest_demand
                         || '|'
                         || '0'
                END;

            v_interest_demand_usd :=
                CASE
                    WHEN v_interest_demand_usd = '' THEN
                        '0'
                    ELSE v_interest_demand_usd
                         || '|'
                         || '0'
                END;

            v_default_interest_demand :=
                CASE
                    WHEN v_default_interest_demand = '' THEN
                        '0'
                    ELSE v_default_interest_demand
                         || '|'
                         || '0'
                END;

            v_default_interest_demand_usd :=
                CASE
                    WHEN v_default_interest_demand_usd = '' THEN
                        '0'
                    ELSE v_default_interest_demand_usd
                         || '|'
                         || '0'
                END;

            v_principal_demand :=
                CASE
                    WHEN v_principal_demand = '' THEN
                        '0'
                    ELSE v_principal_demand
                         || '|'
                         || '0'
                END;

            v_principal_demand_usd :=
                CASE
                    WHEN v_principal_demand_usd = '' THEN
                        '0'
                    ELSE v_principal_demand_usd
                         || '|'
                         || '0'
                END;

            v_demand_date :=
                CASE
                    WHEN v_demand_date = '' THEN
                        ''
                    ELSE v_demand_date
                         || '|'
                         || ''
                END;

            v_overdue_date :=
                CASE
                    WHEN v_overdue_date = '' THEN
                        ''
                    ELSE v_overdue_date
                         || '|'
                         || ''
                END;

            v_clr_bal_amt :=
                CASE
                    WHEN v_clr_bal_amt = '' THEN
                        '0'
                    ELSE v_clr_bal_amt
                         || '|'
                         || '0'
                END;

            v_dmd_eff_date :=
                CASE
                    WHEN v_dmd_eff_date = '' THEN
                        ''
                    ELSE v_dmd_eff_date
                         || '|'
                         || ''
                END;

            v_dmd_ovdu_date :=
                CASE
                    WHEN v_dmd_ovdu_date = '' THEN
                        ''
                    ELSE v_dmd_ovdu_date
                         || '|'
                         || ''
                END;

        END IF;

        
        
        
        
        
        IF
            rec.discounting_boolen = 'Y'
            AND rec.compound_boolen = 'Y'
            AND rec.amortization_boolen = 'Y'
            AND rec.float_boolen = 'Y'
        THEN
            v_comment := 'DiscountAmortizationCompoundingFloat';
        ELSIF
            rec.discounting_boolen = 'Y'
            AND rec.compound_boolen = 'Y'
            AND rec.bullet_boolen = 'Y'
            AND rec.float_boolen = 'Y'
        THEN
            v_comment := 'DiscountBulletCompoundingFloat';
        ELSIF
            rec.compound_boolen = 'Y'
            AND rec.amortization_boolen = 'Y'
            AND rec.float_boolen = 'Y'
        THEN
            v_comment := 'AmortizationCompoundingFloat';
        ELSIF
            rec.compound_boolen = 'N'
            AND rec.amortization_boolen = 'Y'
            AND rec.float_boolen = 'Y'
        THEN
            v_comment := 'AmortizationFloat';

        ELSIF
            rec.compound_boolen = 'Y'
            AND rec.bullet_boolen = 'Y'
            AND rec.float_boolen = 'Y'
        THEN
            v_comment := 'BulletCompoundingFloat';
        ELSIF
            rec.discounting_boolen = 'Y'
            AND rec.bullet_boolen = 'Y'
            AND rec.fixed_boolen = 'Y'
        THEN
            v_comment := 'DiscountBulletFixed';
        ELSIF
            rec.discounting_boolen = 'Y'
            AND rec.bullet_boolen = 'Y'
            AND rec.float_boolen = 'Y'
        THEN
            v_comment := 'DiscountBulletFloat';
        ELSIF
            rec.bullet_boolen = 'Y'
            AND rec.float_boolen = 'Y'
        THEN
            v_comment := 'BulletFloat';
        ELSIF
            rec.bullet_boolen = 'Y'
            AND rec.fixed_boolen = 'Y'
        THEN
            v_comment := 'BulletFixed';
        ELSE
            v_comment := NULL; -- or a default value like 'UnknownStructure'
        END IF;

        v_acct_name := REPLACE(demand_rec.acct_name, ',', '-');
        IF rec.float_boolen = 'Y' THEN
            v_leg_type := 'FLOAT';
        ELSE
            v_leg_type := 'FIXED';
        END IF;

      IF rec.overdue_boolen <> 'Y'
           AND rec.amortization_boolen <> 'N' THEN
        BEGIN
    LOOP
        v_date_part := REGEXP_SUBSTR(v_demand_date, '[^|]+', 1, v_idx);
        v_balance_part := REGEXP_SUBSTR(v_running_balance_s, '[^|]+', 1, v_idx);

        EXIT WHEN v_date_part IS NULL AND v_balance_part IS NULL;

        IF v_output IS NOT NULL AND v_output <> '' THEN
            v_output := v_output || ',';
        END IF;

        IF v_date_part IS NOT NULL  and v_balance_part > 0 THEN
            v_output := v_output ||',' || v_date_part;
        END IF;

         IF (v_date_part IS NOT NULL or v_date_part <> '') and v_idx = 1 THEN
            v_start_date := v_date_part;
        END IF;
        v_end_date := v_prev_date_part;

        IF v_balance_part IS NOT NULL and v_balance_part > 0 THEN
            v_output := v_output || ',' || v_balance_part;
        END IF;

         v_prev_date_part := v_date_part;

        v_idx := v_idx + 1;

    END LOOP;


    END;

    IF
     V_counter < v_idx THEN

    V_counter := v_idx ;
     else V_counter := V_counter;
      v_header   := 'Action,ExternalRefId,TradeId,TradeCounterParty,TradeBook,TradeDateTime,TraderName,SalesPerson,Comment,ProductType,LegType,PayRec,StartDate,EndDate,OpenTermB,BehavioralMaturity,NoticeDays,Currency,Amount,PrincipalExchangeInitialB,PrincipalExchangeFinalB,Rate,FloatingRateReset,RateIndex,RateIndexSource,Tenor,Spread,DayCountConvention,DateRollConvention,PaymentFrequency,DiscountMethod,CouponPaymentAtEndB,AccrualPeriodAdjustment,IncludeFirstB,IncludeLastB,RollDay,HolidayCode,SettlementHolidays,AmountsRounding,RatesRounding,RatesRoundingDecPlaces,IgnoreFullCouponForNotionalAmort,SecuredFundingB,StubPeriod,SpecificFirstDate,SpecificLastDate,CustomStubTolerance,InterestCompounding,InterestCompoundingMethod,CpnHoliday,InterestCompoundingFrequency,ResetLag,CapitalizeB,keyword.1_MM Classification,keyword.FinacleCptyCIFId,keyword.FinacleCptyName,keyword.FinacleCptyType,PrincipalStructure_AmortizationType';
 BEGIN
   FOR i IN 1..V_counter - 2 LOOP
      v_header := v_header || v_header2;
   END LOOP;
END;
     END IF;
    END IF;



        IF
            rec.overdue_boolen <> 'Y'
           AND rec.amortization_boolen <> 'N'
        THEN
             v_header3 :=  v_header3 
                             || v_action
                             || ','
                             || rec.foracid
                             || ','
                             || ''
                             || ','
                             || 'FINACLE_CBS_COUNTERPARTY'
                             || ','
                             || 'CBS_LOANS_DEPOSITS'
                             || ','
                             || v_db_stat_date
                             || ','
                             || 'Finacle'
                             || ','
                             || 'Finacle'
                             || ','
                             || v_comment
                             || ','
                             || 'StructuredFlows'
                             || ','
                             || v_leg_type
                             || ','
                             || 'REC'
                             || ','
                             || to_char(rec.acct_opn_date, 'YYYYMMDD')
                             || ','
                             || to_char(rec.account_end_date, 'YYYYMMDD')
                             || ','
                             || ''
                             || ','
                             || ''
                             || ','
                             || ''
                             || ','
                             || v_acct_crncy_code
                             || ','
                             || abs(rec.tran_date_bal)
                             || ','
                             || 'TRUE'
                             || ','
                             || 'TRUE'
                             || ','
                             || ''
                             || ','
                             ||
                CASE
                    WHEN rec.float_boolen = 'Y' THEN
                        'TRUE'
                    ELSE 'FALSE'
                END
                             || ','
                             || case when v_leg_type = 'FIXED' THEN NULL ELSE rec.rateindex END
                             || ','
                             ||  case when v_leg_type = 'FIXED' THEN NULL ELSE rec.rateindexsource END
                             || ','
                             ||
                CASE
                    WHEN rec.float_boolen = 'Y' THEN
                        rec.tenor
                    ELSE NULL
                END
                             || ','
                             ||
                CASE
                    WHEN rec.float_boolen = 'Y' THEN
                        rec.nominal_margin
                    ELSE NULL
                END
                             || ','
                             || 'ACT/360'
                             || ','
                             || 'MOD_FOLLOW'
                             || ','
                             ||
                CASE
                    WHEN rec.interest_lr_freq_type = 'Quaterly' THEN
                        'QTR'
                    WHEN rec.interest_lr_freq_type = 'Monthly' THEN
                        'MTH'
                    WHEN rec.interest_lr_freq_type = 'Daily' THEN
                        'DLY'
                    WHEN rec.interest_lr_freq_type = 'Half yearly' THEN
                        'SA'
                    WHEN rec.interest_lr_freq_type = 'Yearly' THEN
                        'PA'
                    ELSE NULL
                END
                             || ','
                             ||
                CASE
                    WHEN rec.discounting_boolen = 'Y' THEN
                        'PREPAID'
                    ELSE NULL
                END
                             || ','
                             ||
                CASE
                    WHEN rec.discounting_boolen = 'Y' THEN
                        'TRUE'
                    ELSE 'FALSE'
                END
                             || ','
                             || ''
                             || ','
                             || ''
                             || ','
                             || ''
                             || ','
                             || ''
                             || ','
                             || demand_rec.holiday_calendar
                             || ','
                             || demand_rec.holiday_calendar
                             || ','
                             || 'NEAREST'
                             || ','
                             || 'NEAREST'
                             || ','
                             || 8
                             || ','
                             || ''
                             || ','
                             || ''
                             || ','
                             || case when rec.lr_freq_type_desc = 'Bullet' then null else 'SPECIFIC BOTH' end
                             || ','
                             || case when rec.lr_freq_type_desc = 'Bullet' then null else v_start_date end 
                             || ','
                             || case when rec.lr_freq_type_desc = 'Bullet' then null else V_end_date end  
                             || ','
                             || ''
                             || ','
                             ||
                CASE
                    WHEN rec.compound_boolen = 'Y' THEN
                        'TRUE'
                    ELSE 'FALSE'
                END
                             || ','
                             ||
                CASE
                    WHEN rec.compound_boolen = 'Y' THEN
                        'SimpleSpr'
                    ELSE NULL
                END
                             || ','
                             ||
                CASE
                    WHEN rec.compound_boolen = 'Y' THEN
                        demand_rec.holiday_calendar
                    ELSE NULL
                END
                             || ','
                             ||
                CASE
                    WHEN rec.compound_boolen = 'Y' THEN
                        'DLY'
                    ELSE NULL
                END
                             || ','
                             ||
                CASE
                    WHEN rec.compound_boolen = 'Y' THEN
                        -rec.look_back_period
                    ELSE NULL
                END
                             || ','
                             || NULL
                             || ','
                             || 'Finacle'
                             || ','
                             ||rec.FinacleCptyCIFId ||','||REPLACE(rec.FinacleCptyName, ',', '-')||','||rec.FinacleCptyType || ',' ||
                CASE
                    WHEN rec.lr_freq_type_desc = 'Bullet' THEN
                        'Bullet'
                    ELSE 'Schedule'
                END
                             ||case when rec.lr_freq_type_desc = 'Bullet' then null else v_output end
                             || chr(10);
        END IF;
v_running_balance := 0;
v_output := null;
v_running_balance_s := null;
v_demand_date := null;
 v_idx  := 1;

    END LOOP;




    CLOSE loan_cursor;
   result_output := v_header ||chr(10) ||  v_header3;

    RETURN  result_output;
END get_amort_data;

--set serveroutput on
/**/

--GRANT ALL ON CUSTOM.get_amort_data TO PUBLIC
/


--SELECT custom.get_amort_data() FROM dual;

