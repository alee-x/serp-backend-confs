CREATE SCHEMA SAIL0000V;
-- =============================================
-- || CREATE WDSD TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS SAIL0000V.WDSD_MAIN (
	alf varchar(36) not null, 
	wob DATE not null,
	gndr_cd INT,
	lsoa2011_cd varchar(20)
	);

-- =============================================
-- || CREATE WLGP_REG TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS SAIL0000V.WLGP_REG (
	alf varchar(36) not null,
	start_dt DATE not null,
	end_dt DATE not null
);

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

-- =============================================
-- || CREATE PEDW_SUPERSPELL TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS SAIL0000V.PEDW_SUPERSPELL (
	person_spell_num varchar(36),
	provider_spell_num varchar(36),
	spell_num varchar(36),
	epi_num int
);