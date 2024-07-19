SELECT 
    * 
FROM (
    SELECT 
        row_number() over(ORDER BY NULL) AS rn,
        *
    FROM (
        WITH crm_trading_accounts AS (
            SELECT 
                DISTINCT
                    ta.user_id
                    ,cts.server 
                    ,ta.external_id
                    ,ta.type
                    ,ta.salescode
            FROM (
                SELECT
                    *
                    ,ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) AS RN
                FROM tmgm_datawarehouse.lnd.CRM_TRADING_ACCOUNTS
            ) ta
            LEFT JOIN tmgm_datawarehouse.tst.CRM_SERVERS cts
            ON cts.crm_server_id = ta.type
            WHERE RN = 1 
                AND cts.server LIKE '%%tmgm%%'
                AND cts.server LIKE '%%live%%'
                AND ta.salescode NOT LIKE '%%9999_Test%%'
        ),
		-- login group 
        login_group AS (
            SELECT
                 COALESCE (CAST(rsu.login AS VARCHAR), vc.taker_login) AS login
                ,COALESCE (rsu.server, vc.server) AS server
                ,COALESCE (rsu."GROUP", vc."GROUP") AS "GROUP"
				,COALESCE (rsu.lastdate, vc.lastdate) AS lastdate
            FROM (
                SELECT
                   login
                  ,server
                  ,"GROUP"
				  ,lastdate
                FROM (
                     SELECT
                        login
                       ,server
                       ,"GROUP"
					   ,lastdate
                       ,ROW_NUMBER() OVER(PARTITION BY login, server ORDER BY modify_time DESC) AS rn -- for safety? it's already unique on login + server in TMGM 
                     FROM lnd.report_server_users
                )
                WHERE rn = 1 
            ) AS rsu
            FULL OUTER JOIN ( -- some of the users missed in users table so need pick them up from venue_change table since venue_change at least have 1 record for each login.
                SELECT
                     taker_login
                    ,server
                    ,"GROUP"
					,lastdate
                FROM (
                    SELECT
                         taker_login
                        ,server
                        ,ARRAY_TO_STRING(taker_mt_group_to_array, '-') AS "GROUP"
						,NULL as lastdate -- leave the archived user with null last login
                        ,ROW_NUMBER() OVER(PARTITION BY taker_login, server ORDER BY server_time DESC) AS rn -- latest record
                    FROM tst.venue_change
                )
                WHERE rn=1
            ) AS vc
              ON vc.taker_login = CAST(rsu.login AS VARCHAR)
             AND vc.server = rsu.server
        ),
		-- add group filter
        crm_trading_accounts_group AS (
            SELECT
               ta.user_id
              ,ta.external_id
              ,ta.server
              ,ta.type
              ,ta.salescode
              ,grp."GROUP"
			  ,grp.lastdate
            FROM crm_trading_accounts ta
            LEFT JOIN login_group    grp  -- some of TA in CRM_TRADING_ACCOUNTS not exists in login_group, sample TA id 721854
               ON ta.external_id = grp.login
              AND ta.server = grp.server
			WHERE 1 = 1
			  -- AND grp."GROUP" LIKE ANY ('%%') -- this one should be pass through Mars Filters
		),
		-- get the last login date for a user, if user have 2+ TA, then get the latest login among those TAs
		member_last_login AS (
			SELECT
			   user_id
			  ,MAX(lastdate) AS last_login
			FROM crm_trading_accounts_group
			GROUP BY 1
		),	
        -- T-1 equity in local currency
        equity_report AS (
            SELECT
			   eq.dt_report
			  ,eq.user_id
			  ,eq.login
			  ,eq.currency
              ,eq.positive_equity
            FROM tmgm_datawarehouse.tst.accounting_t_1_equity_report eq
			LEFT JOIN crm_trading_accounts_group    grp -- filter on group
			  ON eq.user_id = grp.user_id
			 AND eq.login = grp.external_id
            WHERE eq.dt_report = DATE_TRUNC('DAY', TST.TO_RTZ(SYSDATE()))
			  AND DATE(CONCAT(CAST(eq.year AS VARCHAR),'-',CAST(eq.month AS VARCHAR),'-',CAST(eq.day AS VARCHAR))) >= DATE_TRUNC('DAY', TST.TO_RTZ(SYSDATE())) - INTERVAL '1 DAY'
			  AND eq.user_id IS NOT NULL
			  AND eq.currency IS NOT NULL
			  AND eq.login NOT LIKE 'W%'
  			  -- AND grp."GROUP" IS NOT NULL -- need Mars add a new condition check if end user filter on GROUP, if yes, then need apply this condition
			  
        ),
        -- daily level symbol/currency conversion ratio
        ex_rate AS (
            SELECT DISTINCT
               DATE(pricing.timereceived_server) AS dt_report -- every day will only have 1 close
              ,pricing.core_symbol
              ,symbol_spec.symbol
              ,CASE
                 WHEN symbol_spec.symbol = 'USDCNH' THEN 'CNH'
                 WHEN symbol_spec.symbol = 'EURUSD' THEN 'EUR'
                 WHEN symbol_spec.symbol = 'NZDUSD' THEN 'NZD'
                 WHEN symbol_spec.symbol = 'GBPUSD' THEN 'GBP'
                 WHEN symbol_spec.symbol = 'USDHKD' THEN 'HKD'
                 WHEN symbol_spec.symbol = 'AUDUSD' THEN 'AUD'
                 WHEN symbol_spec.symbol = 'USDCAD' THEN 'CAD'
                 WHEN symbol_spec.symbol = 'USDCHF' THEN 'CHF'
                 WHEN symbol_spec.symbol = 'USDJPY' THEN 'JPY'
                 ELSE 'UNK'
               END AS ac
              ,FIRST_VALUE(pricing.close) OVER(PARTITION BY pricing.core_symbol, DATE(pricing.timereceived_server)
                  ORDER BY pricing.timereceived_server DESC) AS close
              ,symbol_spec.contractsize
              ,symbol_spec.basecurrency
              ,symbol_spec.type
            FROM tmgm_datawarehouse.lnd.pricing_ohlc_minute  AS pricing
            LEFT JOIN tmgm_datawarehouse.lnd.symbol_settings AS symbol_spec -- get the symbol specification bases on received time
              ON replace(pricing.core_symbol,'/','') = symbol_spec.symbol
             AND pricing.TIMERECEIVED_SERVER::DATE >= symbol_spec.START_DATE
             AND pricing.TIMERECEIVED_SERVER::DATE <= symbol_spec.LAST_ACTIVE
            WHERE pricing.core_symbol IN ('EUR/USD','AUD/USD','GBP/USD','USD/CAD','USD/HKD','NZD/USD','USD/CNH','USD/CHF','USD/JPY') -- limit to forex symbol
              AND symbol_spec.symbol IS NOT NULL
			  AND pricing.TIMERECEIVED_SERVER::DATE <= DATE_TRUNC('DAY', TST.TO_RTZ(SYSDATE())) - INTERVAL '14 DAY' -- filter for performance
        ),
        -- convert to USD and aggreate to user level
        member_true_equity AS (
            SELECT
               user_id
              ,ROUND(SUM(CASE
                           WHEN currency = 'USD' THEN positive_equity * 1.0
                           WHEN SUBSTRING(symbol,1,3) = currency THEN positive_equity * close -- e.g. AUDUSD:0.66, USDAUD:1.55
                           WHEN SUBSTRING(symbol,-3) = currency THEN positive_equity / close
                           ELSE 0.0
                        END),2) AS true_equity_usd
            FROM (
                SELECT
                   t1.dt_report
                  ,t1.user_id
                  ,t1.login
                  ,t1.currency
                  ,t1.positive_equity
                  ,ex.close
                  ,ex.symbol
                  ,ROW_NUMBER() OVER (PARTITION BY t1.dt_report, t1.user_id, t1.login, t1.currency ORDER BY ex.dt_report DESC) AS rn
                FROM equity_report  t1
                LEFT JOIN ex_rate   ex
                  ON t1.currency = ex.ac
                 AND ex.dt_report <= DATE(t1.dt_report) - INTERVAL '1 DAY'
                 AND ex.dt_report >= DATE(t1.dt_report) - INTERVAL '6 DAY' -- will 1:m join, so need dedup, look back for 7 days
                QUALIFY rn = 1
            )
            GROUP BY 1
        ),
        crm_transactions AS (
          SELECT
            *
          FROM
            (
              SELECT
                trx.user_id,
                trx.trading_server_comment,
                trx.fund_currency,
                trx.completed_at,
                trx.fund_amount,
                trx.usd_exrate_amount,
                trx.usd_fund_amount,
                ROW_NUMBER() OVER(
                  PARTITION BY trx.id
                  ORDER BY
                    trx.updated_at DESC
                ) AS RN
              FROM
                tmgm_datawarehouse.lnd.CRM_TRANSACTIONS trx
                LEFT JOIN crm_trading_accounts_group   ta --  filter group
                   ON trx.user_id = ta.user_id
                  AND trx.external_id = ta.external_id
                WHERE trx.status = 2
                  AND lower(trx.no) not like '%%test%%'
                  AND trx.completed_at IS NOT NULL
				  -- AND grp."GROUP" IS NOT NULL -- need Mars add a new condition check if end user filter on GROUP, if yes, then need apply this condition
            )
          WHERE
            RN = 1
        ),
        member_transactions AS (
            SELECT
                user_id,
                SUM(deposits) AS total_dep,
                SUM(withdrawals) AS total_wid,
                SUM(ndp) AS total_ndp,
                MAX(dep_time) AS last_dep,
                MAX(wid_time) AS last_wid,
                MAX(ndp_time) AS last_ndp
            FROM
                (
                SELECT
                    user_id,CASE
                    WHEN ((LOWER(trading_server_comment) LIKE '%deposit%' AND LOWER(trading_server_comment) NOT LIKE '%bonus%' AND LOWER(trading_server_comment) NOT LIKE '%reopen%') 
                            OR (LOWER(trading_server_comment) LIKE '%dp%' AND LOWER(trading_server_comment) NOT LIKE '%withdraw%')
                            OR (LOWER(trading_server_comment) LIKE '%sub%' AND LOWER(trading_server_comment) NOT LIKE '%wd%' AND LOWER(trading_server_comment) NOT LIKE '%withdraw%' AND LOWER(trading_server_comment) NOT LIKE '%[%]%') 
                            OR (LOWER(trading_server_comment) LIKE '%internal%tf%from%' AND LOWER(trading_server_comment) LIKE '%dep%')
                            OR (LOWER(trading_server_comment) LIKE '%internal%tf%fm%' AND LOWER(trading_server_comment) LIKE '%dep%')) THEN (
                        CASE
                        WHEN (fund_currency = 'USD')
                        AND (
                            completed_at < cast('2020-12-12 14:00:00' as timestamp)
                        ) THEN CAST(fund_amount AS DOUBLE)
                        ELSE CAST(usd_exrate_amount AS DOUBLE) + CAST(usd_fund_amount AS DOUBLE)
                        END
                    )
                    END AS deposits,CASE
                    WHEN (LOWER(trading_server_comment) LIKE '%withdraw%' OR LOWER(trading_server_comment) LIKE '%wd%') THEN (
                        CASE
                        WHEN (fund_currency = 'USD')
                        AND (
                            completed_at < cast('2020-12-12 14:00:00' as timestamp)
                        ) THEN CAST(fund_amount AS DOUBLE)
                        ELSE CAST(usd_exrate_amount AS DOUBLE) + CAST(usd_fund_amount AS DOUBLE)
                        END
                    )
                    END AS withdrawals,CASE
                    WHEN (
                            (LOWER(trading_server_comment) LIKE '%deposit%' AND LOWER(trading_server_comment) NOT LIKE '%bonus%' AND LOWER(trading_server_comment) NOT LIKE '%reopen%') 
                            OR (LOWER(trading_server_comment) LIKE '%dp%' AND LOWER(trading_server_comment) NOT LIKE '%withdraw%')
                            OR (LOWER(trading_server_comment) LIKE '%sub%' AND LOWER(trading_server_comment) NOT LIKE '%wd%' AND LOWER(trading_server_comment) NOT LIKE '%withdraw%' AND LOWER(trading_server_comment) NOT LIKE '%[%]%')
                        )
                        OR (
                            LOWER(trading_server_comment) LIKE '%withdraw%' 
                            OR (LOWER(trading_server_comment) LIKE '%wd%' AND LOWER(trading_server_comment) NOT LIKE '%internal%tf%')) THEN (
                        CASE
                        WHEN (fund_currency = 'USD')
                        AND (
                            completed_at < cast('2020-12-12 14:00:00' as timestamp)
                        ) THEN CAST(fund_amount AS DOUBLE)
                        ELSE CAST(usd_exrate_amount AS DOUBLE) + CAST(usd_fund_amount AS DOUBLE)
                        END
                    )
                    END AS ndp,CASE
                    WHEN LOWER(trading_server_comment) like '%%deposit%%'
                    OR LOWER(trading_server_comment) like '%%internal tf dep from ib%%' THEN completed_at
                    END AS dep_time,CASE
                    WHEN LOWER(trading_server_comment) like '%%withdrawal%%'
                    OR LOWER(trading_server_comment) like '%%internal tf wd to ib%%' THEN completed_at
                    END AS wid_time,CASE
                    WHEN LOWER(trading_server_comment) like '%%deposit%%'
                    OR LOWER(trading_server_comment) like '%%internal tf dep from ib%%'
                    OR LOWER(trading_server_comment) like '%%withdrawal%%'
                    OR LOWER(trading_server_comment) like '%%internal tf wd to ib%%' THEN completed_at
                    END AS ndp_time
                FROM
                    crm_transactions
                )
            GROUP BY
                user_id
                -- {15}
                -- {17}
		),
        member_last_trade AS (
            SELECT 
                user_id
                ,'last_trade' AS event_type
                ,MAX(ltt.last_trade_time) AS last_trade_time
            FROM (
                SELECT 
                    login
                    ,server
                    ,MAX(last_open_time) AS last_trade_time
                FROM (
                    SELECT 
                        login
                        ,server
                        ,MAX(open_time) AS last_open_time
                    FROM tmgm_datawarehouse.lnd.REPORT_SERVER_TRADES_CLOSED
                    WHERE cmd IN (0, 1)
                        AND server NOT IN ('mt4_tmgm_demo','mt4_tmgm_uat','mt4_sam_live01','mt4_ttg_live01','mt4_uw_live01')
                    GROUP BY login, server
                    UNION ALL
                    SELECT 
                        login
                        ,server
                        ,MAX(open_time) AS last_open_time
                    FROM tmgm_datawarehouse.lnd.REPORT_SERVER_TRADES_OPEN
                    WHERE cmd IN (0, 1)
                        AND server NOT IN ('mt4_tmgm_demo','mt4_tmgm_uat','mt4_sam_live01','mt4_ttg_live01','mt4_uw_live01')
                    GROUP BY login, server
                ) AS trades
                GROUP BY login, server
            ) AS ltt
            LEFT JOIN crm_trading_accounts_group AS cta
            ON cta.external_id = CAST(ltt.login AS VARCHAR(50))
                AND cta.server = ltt.server    
            WHERE ltt.last_trade_time IS NOT NULL
			-- AND grp."GROUP" IS NOT NULL -- need Mars add a new condition check if end user filter on GROUP, if yes, then need apply this condition
            --{16}
            GROUP BY user_id
        )
        -- {0}
        -- {1}
        SELECT
            DISTINCT
            ct.user_id,
            COALESCE(ROUND(member_transactions.total_dep, 2), 0) AS member_total_deposit,
            COALESCE(ROUND(member_transactions.total_wid, 2), 0) AS member_total_withdrawal,
            COALESCE(ROUND(member_transactions.total_ndp, 2), 0) AS member_total_ndp,
            last_dep AS member_last_deposit,
            last_wid AS member_last_withdrawal,
            last_ndp AS member_last_deposit_or_withdrawal,
            last_trade_time AS member_last_trade,
            true_equity_usd AS member_true_equity,
            ROUND(COALESCE(total_ndp, 0) - COALESCE(true_equity_usd, 0), 2) AS member_true_equity_change,
            last_login AS member_last_login
        FROM
            crm_trading_accounts ct
        LEFT JOIN member_transactions
            ON ct.user_id = member_transactions.user_id
        LEFT JOIN member_last_trade
            ON ct.user_id = member_last_trade.user_id
        LEFT JOIN member_true_equity 
            On ct.user_id = member_true_equity.user_id
		LEFT JOIN member_last_login
		    On ct.user_id = member_last_login.user_id
        -- {6}
        -- {7}
        WHERE true
            -- {8}
            -- {9}
            -- {3}
            -- {4}
            -- {5}
        ORDER BY ct.user_id
    )
)
-- WHERE rn > {2}
LIMIT 25