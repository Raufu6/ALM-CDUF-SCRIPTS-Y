BEGIN
    -- Drop the table if it exists
    EXECUTE IMMEDIATE 'DROP TABLE custom.GETAMORT_DEMAND';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN  -- ORA-00942: table or view does not exist
            RAISE;
        END IF;
END;
/


create table custom.GETAMORT_DEMAND AS SELECT
        sysdate system_date,
        (
            SELECT
                db_stat_date - 1
            FROM
                tbaadm.gct
        )       db_stat_date,
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
        (
            SELECT
--'1' b,
--(select ll.LIMIT_PREFIX||'/'|| ll.LIMIT_SUFFIX from tbaadm.llt ll where  del_flg='N' and  ll.LIMIT_B2KID in  (select PARENT_LIMIT_B2KID from tbaadm.llt where llt.LIMIT_B2KID=gam.LIMIT_B2KID and del_flg='N')) as "PARENT LIMIT",
                (
                    SELECT
                        limit_prefix
                        || '/'
                        || limit_suffix
                    FROM
                        tbaadm.llt
                    WHERE
                        limit_suffix LIKE 'FAC%'
                    CONNECT BY
                        limit_b2kid = PRIOR parent_limit_b2kid
                    START WITH limit_b2kid IN ( gam.limit_b2kid )
                )               parent_limit,
                gam.acct_crncy_code,
                gam.foracid     AS "ACCOUNT_NO",
                nvl(
                    decode(dmd_flow_id, 'INDEM', dmd_amt - tot_adj_amt),
                    0
                )               AS "INTEREST_DEMAND",
                nvl(
                    custom.all_report_functions.get_new_currency_val((decode(dmd_flow_id, 'INDEM', dmd_amt - tot_adj_amt)),
                                                                     gam.acct_crncy_code,
                                                                     'USD',
                                                                     (
                                                                  SELECT
                                                                      db_stat_date
                                                                  FROM
                                                                      tbaadm.gct
                                                              )),
                    0
                )               interest_demand_usd,
                nvl(
                    decode(dmd_flow_id, 'PIDEM', dmd_amt - tot_adj_amt),
                    0
                )               AS "DEFAULT_INTEREST_DEMAND",
                nvl(
                    custom.all_report_functions.get_new_currency_val((decode(dmd_flow_id, 'PIDEM', dmd_amt - tot_adj_amt)),
                                                                     gam.acct_crncy_code,
                                                                     'USD',
                                                                     (
                                                                  SELECT
                                                                      db_stat_date
                                                                  FROM
                                                                      tbaadm.gct
                                                              )),
                    0
                )               default_interest_demand_usd,
                nvl(
                    decode(dmd_flow_id, 'PRDEM', dmd_amt - tot_adj_amt),
                    0
                )               AS "PRINCIPAL_DEMAND",
                nvl(
                    custom.all_report_functions.get_new_currency_val((decode(dmd_flow_id, 'PRDEM', dmd_amt - tot_adj_amt)),
                                                                     gam.acct_crncy_code,
                                                                     'USD',
                                                                     (
                                                                  SELECT
                                                                      db_stat_date
                                                                  FROM
                                                                      tbaadm.gct
                                                              )),
                    0
                )               principal_demand_usd,
                dmd_eff_date    AS "DEMAND_DATE",
                dmd_ovdu_date   AS "OVERDUE_DATE",
                gam.clr_bal_amt clr_bal_amt,
                dmd_eff_date    dmd_eff_date,
                dmd_ovdu_date   dmd_ovdu_date,
                holiday_calendar,
                acct_name
            FROM
                tbaadm.ldt,
                tbaadm.gam,
                tbaadm.lam,
                curr_hol_mapping c
            WHERE
                    ldt.acid = gam.acid
                AND lam.acid = gam.acid
                AND gam.acct_crncy_code = c.curreny
                AND lam.payoff_flg != 'Y'
--and gam.clr_bal_amt!=0
                AND ldt.dmd_amt != tot_adj_amt
                AND dmd_ovdu_date < (
                    SELECT
                        db_stat_date
                    FROM
                        tbaadm.gct
                )
--and gam.foracid ='400002701300000'
                AND gam.schm_code != '900'
                AND NOT EXISTS (
                    SELECT
                        1
                    FROM
                        tbaadm.cot
                    WHERE
                            charge_off_type = 'F'
                        AND cot.acid = gam.acid
                )
            UNION

---------------------------------- deferred principal and interest
            SELECT
--'11' b,
--(select ll.LIMIT_PREFIX||'/'|| ll.LIMIT_SUFFIX from tbaadm.llt ll where  del_flg='N' and  ll.LIMIT_B2KID in  (select PARENT_LIMIT_B2KID from tbaadm.llt where llt.LIMIT_B2KID=gam.LIMIT_B2KID and del_flg='N')) as "PARENT LIMIT",
                (
                    SELECT
                        limit_prefix
                        || '/'
                        || limit_suffix
                    FROM
                        tbaadm.llt
                    WHERE
                        limit_suffix LIKE 'FAC%'
                    CONNECT BY
                        limit_b2kid = PRIOR parent_limit_b2kid
                    START WITH limit_b2kid IN ( gam.limit_b2kid )
                )             parent_limit,
                gam.acct_crncy_code,
                gam.foracid   AS "ACCOUNT_NO",
                nvl(
                    decode(dmd_flow_id, 'INDEM', dmd_amt - tot_adj_amt),
                    0
                )             AS "INTEREST_DEMAND",
                nvl(
                    custom.all_report_functions.get_new_currency_val((decode(dmd_flow_id, 'INDEM', dmd_amt - tot_adj_amt)),
                                                                     gam.acct_crncy_code,
                                                                     'USD',
                                                                     (
                                                                  SELECT
                                                                      db_stat_date
                                                                  FROM
                                                                      tbaadm.gct
                                                              )),
                    0
                )             interest_demand_usd,
                nvl(
                    decode(dmd_flow_id, 'PIDEM', dmd_amt - tot_adj_amt),
                    0
                )             AS "DEFAULT_INTEREST_DEMAND",
                nvl(
                    custom.all_report_functions.get_new_currency_val((decode(dmd_flow_id, 'PIDEM', dmd_amt - tot_adj_amt)),
                                                                     gam.acct_crncy_code,
                                                                     'USD',
                                                                     (
                                                                  SELECT
                                                                      db_stat_date
                                                                  FROM
                                                                      tbaadm.gct
                                                              )),
                    0
                )             default_interest_demand_usd,
                nvl(
                    decode(dmd_flow_id, 'PRDEM', dmd_amt - tot_adj_amt),
                    0
                )             AS "PRINCIPAL_DEMAND",
                nvl(
                    custom.all_report_functions.get_new_currency_val((decode(dmd_flow_id, 'PRDEM', dmd_amt - tot_adj_amt)),
                                                                     gam.acct_crncy_code,
                                                                     'USD',
                                                                     (
                                                                  SELECT
                                                                      db_stat_date
                                                                  FROM
                                                                      tbaadm.gct
                                                              )),
                    0
                )             principal_demand_usd,
                dmd_eff_date  AS "DEMAND_DATE",
                dmd_ovdu_date AS "OVERDUE_DATE",
                gam.clr_bal_amt,
                dmd_eff_date  dmd_eff_date,
                dmd_ovdu_date dmd_ovdu_date,
                holiday_calendar,
                acct_name
            FROM
                tbaadm.ldt,
                tbaadm.gam,
                tbaadm.lam,
                curr_hol_mapping c
            WHERE
                    ldt.acid = gam.acid
                AND lam.acid = gam.acid
                AND gam.acct_crncy_code = c.curreny
                AND lam.payoff_flg != 'Y'
--and gam.clr_bal_amt!=0
                AND ldt.dmd_amt != tot_adj_amt
                AND dmd_ovdu_date > (
                    SELECT
                        db_stat_date
                    FROM
                        tbaadm.gct
                )
--and gam.foracid ='400002701300000'
                AND gam.schm_code != '900'
                AND NOT EXISTS (
                    SELECT
                        1
                    FROM
                        tbaadm.cot
                    WHERE
                            charge_off_type = 'F'
                        AND cot.acid = gam.acid
                )
            UNION
            SELECT
--'2' b,
--(select ll.LIMIT_PREFIX||'/'|| LIMIT_SUFFIX from tbaadm.llt ll where del_flg='N' and  LIMIT_B2KID in  (select PARENT_LIMIT_B2KID
--from tbaadm.llt where llt.LIMIT_B2KID=gam.LIMIT_B2KID and del_flg='N')) as "PARENT LIMIT",
                (
                    SELECT
                        limit_prefix
                        || '/'
                        || limit_suffix
                    FROM
                        tbaadm.llt
                    WHERE
                        limit_suffix LIKE 'FAC%'
                    CONNECT BY
                        limit_b2kid = PRIOR parent_limit_b2kid
                    START WITH limit_b2kid IN ( gam.limit_b2kid )
                )               parent_limit,
                gam.acct_crncy_code,
                c_amort.foracid AS "ACCOUNT NO",
                interest_amt    AS "INTEREST DEMAND",
                nvl(
                    custom.all_report_functions.get_new_currency_val(interest_amt, gam.acct_crncy_code, 'USD',(
                        SELECT
                            db_stat_date
                        FROM
                            tbaadm.gct
                    )),
                    0
                )               interest_demand_usd,--PENAL_AMT
                penal_amt       AS "DEFAULT INTEREST DEMAND",
                nvl(
                    custom.all_report_functions.get_new_currency_val(penal_amt, gam.acct_crncy_code, 'USD',(
                        SELECT
                            db_stat_date
                        FROM
                            tbaadm.gct
                    )),
                    0
                )               default_interest_demand_usd,--PENAL_AMT
                principle_amt   AS "PRINCIPAL DEMAND",
                nvl(
                    custom.all_report_functions.get_new_currency_val(principle_amt, gam.acct_crncy_code, 'USD',(
                        SELECT
                            db_stat_date
                        FROM
                            tbaadm.gct
                    )),
                    0
                )               principal_demand_usd,
                flow_date       AS "DEMAND_DATE",
                (
                    SELECT
                        db_stat_date
                    FROM
                        tbaadm.gct
                ) - 1           AS "OVERDUE_DATE",
                gam.clr_bal_amt,
                flow_date       dmd_eff_date,
                NULL            dmd_ovdu_date,
                holiday_calendar,
                acct_name
            FROM
                custom.c_amort,
                tbaadm.gam       gam,
                tbaadm.lam,
                curr_hol_mapping c
            WHERE
                    gam.foracid = c_amort.foracid
                AND lam.acid = gam.acid
                AND gam.acct_crncy_code = c.curreny
                AND lam.payoff_flg != 'Y'
                AND gam.schm_code != '900'
                AND gam.clr_bal_amt != 0
--and gam.foracid ='400002701300000'
--and FLOW_DATE >=(select max(DMD_EFF_DATE) from tbaadm.ldt where  acid in (select acid from tbaadm.gam where foracid =c_amort.foracid))--4174
                AND flow_date >= (
                    SELECT
                        db_stat_date
                    FROM
                        tbaadm.gct
                )
                AND NOT EXISTS (
                    SELECT
                        1
                    FROM
                        tbaadm.cot
                    WHERE
                            charge_off_type = 'F'
                        AND cot.acid = gam.acid
                )

---------------------------------------------------------------------------------------------------------------FBA
            UNION
            SELECT
--'4' b,

                (
                    SELECT
                        limit_prefix
                        || '/'
                        || limit_suffix
                    FROM
                        tbaadm.llt
                    WHERE
                        limit_suffix LIKE 'FAC%'
                    CONNECT BY
                        limit_b2kid = PRIOR parent_limit_b2kid
                    START WITH limit_b2kid IN ( m.limit_b2kid )
                )                 parent_limit,
--(SELECT LIMIT_PREFIX ||'/'|| LIMIT_SUFFIX FROM TBAADM.LLT L1 WHERE del_flg ='N' and  L1.LIMIT_B2KID =(select PARENT_LIMIT_B2KID from tbaadm.llt where del_flg ='N' and llt.LIMIT_B2KID = (select OUR_PARTY_LIMIT_B2KID from tbaadm.dcmm
--where DC_REF_NUM= lc_number) and PARENT_LIMIT_B2KID!='ROOT')) parent_limit,

                m.acct_crncy_code currency,
                f.bill_id         AS "ACCOUNT NO",
                0                 AS "INTEREST DEMAND",
                0                 AS "INTEREST_DEMAND_USD",
                0                 AS "DEFAULT INTEREST DEMAND",
                0                 AS default_interest_demand_usd,
                f.bill_liab       AS "PRINCIPAL DEMAND",
                nvl(
                    custom.all_report_functions.get_new_currency_val(f.bill_liab, m.acct_crncy_code, 'USD',(
                        SELECT
                            db_stat_date
                        FROM
                            tbaadm.gct
                    )),
                    0
                )                 principal_demand_usd,
                f.due_date        AS "DEMAND_DATE",
                f.due_date        AS "OVERDUE_DATE",
                m.clr_bal_amt,
                f.due_date        dmd_eff_date,
                NULL              dmd_ovdu_date,
                holiday_calendar,
                acct_name
            FROM
                tbaadm.gam       m,
                tbaadm.fbh       h,
                tbaadm.fbm       f,
--TBAADM.LLT L
                tbaadm.fei,
                curr_hol_mapping c
            WHERE
                    f.bill_id = h.bill_id
                AND fei.bill_id = f.bill_id
                AND fei.sol_id = f.sol_id
                AND m.acid = h.event_acid
                AND m.acct_crncy_code = c.curreny
                AND m.entity_cre_flg = 'Y'
                AND m.del_flg = 'N'
--and M.Clr_Bal_Amt !=0
                AND h.bill_func = 'P'
                AND h.entity_cre_flg = 'Y'
                AND h.del_flg = 'N'
                AND NOT EXISTS (
                    SELECT
                        1
                    FROM
                        tbaadm.cot
                    WHERE
                            charge_off_type = 'F'
                        AND cot.acid = m.acid
                )
--and F.bill_id =''
--AND F.DUE_DATE > =(select db_stat_date from tbaadm.gct)

            UNION

------------------------------------------------------------------------------ Account balance  zero and some over due is present

            SELECT
--'5' b ,
--(select ll.LIMIT_PREFIX||'/'|| ll.LIMIT_SUFFIX from tbaadm.llt ll where  del_flg='N' and  ll.LIMIT_B2KID in  (select PARENT_LIMIT_B2KID from tbaadm.llt where llt.LIMIT_B2KID=gam.LIMIT_B2KID and del_flg='N')) as "PARENT LIMIT",
                (
                    SELECT
                        limit_prefix
                        || '/'
                        || limit_suffix
                    FROM
                        tbaadm.llt
                    WHERE
                        limit_suffix LIKE 'FAC%'
                    CONNECT BY
                        limit_b2kid = PRIOR parent_limit_b2kid
                    START WITH limit_b2kid IN ( gam.limit_b2kid )
                )             parent_limit,
                gam.acct_crncy_code,
                gam.foracid   AS "ACCOUNT_NO",
                nvl(
                    decode(dmd_flow_id, 'INDEM', dmd_amt - tot_adj_amt),
                    0
                )             AS "INTEREST_DEMAND",
                nvl(
                    custom.all_report_functions.get_new_currency_val((decode(dmd_flow_id, 'INDEM', dmd_amt - tot_adj_amt)),
                                                                     gam.acct_crncy_code,
                                                                     'USD',
                                                                     (
                                                                  SELECT
                                                                      db_stat_date
                                                                  FROM
                                                                      tbaadm.gct
                                                              )),
                    0
                )             interest_demand_usd,
                nvl(
                    decode(dmd_flow_id, 'PIDEM', dmd_amt - tot_adj_amt),
                    0
                )             AS "DEFAULT_INTEREST_DEMAND",
                nvl(
                    custom.all_report_functions.get_new_currency_val((decode(dmd_flow_id, 'PIDEM', dmd_amt - tot_adj_amt)),
                                                                     gam.acct_crncy_code,
                                                                     'USD',
                                                                     (
                                                                  SELECT
                                                                      db_stat_date
                                                                  FROM
                                                                      tbaadm.gct
                                                              )),
                    0
                )             default_interest_demand_usd,
                nvl(
                    decode(dmd_flow_id, 'PRDEM', dmd_amt - tot_adj_amt),
                    0
                )             AS "PRINCIPAL_DEMAND",
                nvl(
                    custom.all_report_functions.get_new_currency_val((decode(dmd_flow_id, 'PRDEM', dmd_amt - tot_adj_amt)),
                                                                     gam.acct_crncy_code,
                                                                     'USD',
                                                                     (
                                                                  SELECT
                                                                      db_stat_date
                                                                  FROM
                                                                      tbaadm.gct
                                                              )),
                    0
                )             principal_demand_usd,
                dmd_eff_date  AS "DEMAND_DATE",
                dmd_ovdu_date AS "OVERDUE_DATE",
                gam.clr_bal_amt,
                dmd_eff_date  dmd_eff_date,
                dmd_ovdu_date dmd_ovdu_date,
                holiday_calendar,
                acct_name
            FROM
                tbaadm.ldt,
                tbaadm.gam,
                tbaadm.lam,
                curr_hol_mapping c
            WHERE
                    ldt.acid = gam.acid
                AND lam.acid = gam.acid
                AND gam.acct_crncy_code = c.curreny
                AND lam.payoff_flg != 'Y'
--and gam.clr_bal_amt  != 0
                AND ldt.dmd_amt != tot_adj_amt
                AND dmd_ovdu_date < (
                    SELECT
                        db_stat_date
                    FROM
                        tbaadm.gct
                )
--and gam.foracid ='400002701300000'
                AND NOT EXISTS (
                    SELECT
                        1
                    FROM
                        tbaadm.cot
                    WHERE
                            charge_off_type = 'F'
                        AND cot.acid = gam.acid
                )
                AND gam.schm_code != '900'
        ) a
    WHERE
        NOT EXISTS (
            SELECT
                1
            FROM
                custom.view_cot_chrge_off_new cot
            WHERE
                cot.parent_id = a.parent_limit
        )
           ---- AND account_no = '600084202410002'
    ORDER BY
        demand_date;
        
        
create index idx_acct on custom.GETAMORT_DEMAND(account_no);
        