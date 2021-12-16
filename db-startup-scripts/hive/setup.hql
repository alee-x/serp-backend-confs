create database SAIL0000V;
use SAIL0000V;

CREATE EXTERNAL TABLE IF NOT EXISTS wdsd_main (
	alf varchar(36), 
	wob DATE,
	gndr_cd INT,
	lsoa2011_cd varchar(20)
	)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/project/SAIL0000V/wdsd_main'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS wlgp_reg (
	alf varchar(36),
	start_dt DATE,
	end_dt DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/project/SAIL0000V/wlgp_reg'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS WLGP_EVENT (
	alf varchar(36),
	gndr_cd INT,
	wob DATE,
	event_dt DATE,
	event_cd varchar(36)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/project/SAIL0000V/wlgp_event'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS PEDW_EPISODE (
	spell_num varchar(36),
	epi_num int,
	epi_start DATE,
	epi_end date,
	epi_duration int,
	first_epi_in_spell int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/project/SAIL0000V/pedw_episode'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS PEDW_SPELL (
	spell_num varchar(36),
	gndr_cd int,
	admis_yr int,
	admis_dt date,
	disch_yr int,
	disch_date date,
	spell_duration int,
	alf varchar(36)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/project/SAIL0000V/pedw_spell'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS PEDW_SUPERSPELL (
	person_spell_num varchar(36),
	provider_spell_num varchar(36),
	spell_num varchar(36),
	epi_num int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/project/SAIL0000V/pedw_superspell'
TBLPROPERTIES ("skip.header.line.count"="1");