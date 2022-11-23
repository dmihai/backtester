# backtester

Tool for backtesting trading strategies

## Common SQL queries

    SELECT jobs.id, jobs.asset, jobs.year, jobs.params, results.*
    FROM `jobs`
    INNER JOIN results ON jobs.id=results.job_id
    WHERE jobs.version=4 AND jobs.status='done' AND results.session='all'
    ORDER BY asset, year, params;
