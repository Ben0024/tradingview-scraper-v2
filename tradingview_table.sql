CREATE TABLE symbol (
    symbol_id SERIAl PRIMARY KEY,
    symbol_name character varying(64) NOT NULL,
    symbol_data TEXT
);

\COPY (SELECT * FROM symbol WHERE symbol_id IN (SELECT symbol_id FROM user_reg_symbol)) TO '/home/ryan/Work/tradingview_scraper_backup/dump_file.txt' WITH CSV HEADER;

\COPY symbol FROM './dump_file.txt' WITH CSV HEADER;