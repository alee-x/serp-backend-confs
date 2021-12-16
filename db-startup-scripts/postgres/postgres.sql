-- =============================================
-- || CREATE WDSD TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS SAIL0000V.WDSD_MAIN (
	alf varchar(36) not null, 
	wob DATE not null,
	gndr_cd INT,
	lsoa2011_cd varchar(20)
);

COPY SAIL0000V.WDSD_MAIN(alf, wob, gndr_cd, lsoa2011_cd)
FROM '/docker-entrypoint-initdb.d/data/wdsd_main.csv'
DELIMITER ','
CSV HEADER;

-- =============================================
-- || CREATE WLGP_REG TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS SAIL0000V.WLGP_REG (
	alf varchar(36) not null,
	start_dt DATE not null,
	end_dt DATE not null
);

COPY SAIL0000V.WLGP_REG(alf, start_dt, end_dt)
FROM '/docker-entrypoint-initdb.d/data/wlgp_reg.csv'
DELIMITER ','
CSV HEADER;

-- =============================================
-- || CREATE WLGP_EVENT TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS SAIL0000V.WLGP_EVENT (
	alf varchar(36) not null,
	gndr_cd INT,
	wob DATE,
	event_dt DATE,
	event_cd varchar(36)
);

COPY SAIL0000V.WLGP_EVENT(alf, gndr_cd, wob, event_dt, event_cd)
FROM '/docker-entrypoint-initdb.d/data/wlgp_event.csv'
DELIMITER ','
CSV HEADER;

-- =============================================
-- || CREATE PEDW_EPISODE TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS SAIL0000V.PEDW_EPISODE (
	spell_num varchar(36),
	epi_num int,
	epi_start DATE,
	epi_end date,
	epi_duration int,
	first_epi_in_spell int
);

COPY SAIL0000V.PEDW_EPISODE(spell_num, epi_num, epi_start, epi_end, epi_duration, first_epi_in_spell)
FROM '/docker-entrypoint-initdb.d/data/pedw_episode.csv'
DELIMITER ','
CSV HEADER;

-- =============================================
-- || CREATE PEDW_SPELL TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS SAIL0000V.PEDW_SPELL (
	spell_num varchar(36),
	gndr_cd int,
	admis_yr int,
	admis_dt date,
	disch_yr int,
	disch_date date,
	spell_duration int,
	alf varchar(36)
);

COPY SAIL0000V.PEDW_SPELL(spell_num, gndr_cd, admis_yr, admis_dt, disch_yr, disch_date, spell_duration, alf)
FROM '/docker-entrypoint-initdb.d/data/pedw_spell.csv'
DELIMITER ','
CSV HEADER;

-- =============================================
-- || CREATE PEDW_SUPERSPELL TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS SAIL0000V.PEDW_SUPERSPELL (
	person_spell_num varchar(36),
	provider_spell_num varchar(36),
	spell_num varchar(36),
	epi_num int
);

COPY SAIL0000V.PEDW_SUPERSPELL(person_spell_num, provider_spell_num, spell_num, epi_num)
FROM '/docker-entrypoint-initdb.d/data/pedw_superspell.csv'
DELIMITER ','
CSV HEADER;