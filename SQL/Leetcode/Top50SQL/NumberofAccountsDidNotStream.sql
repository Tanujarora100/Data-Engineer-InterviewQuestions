SELECT
    COUNT(ACCOUNT_ID) AS ACCOUNTS_COUNT
FROM
    SUBSCRIPTIONS
WHERE
    2021 BETWEEN YEAR(START_DATE)
    AND YEAR(END_DATE)
    AND ACCOUNT_ID NOT IN (
        select
            account_id
        from
            streams
        where
            year(stream_date) = 2021
    )