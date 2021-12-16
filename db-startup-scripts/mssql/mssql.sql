IF NOT EXISTS (SELECT * FROM sys.databases WHERE name='SAIL')
BEGIN
	CREATE DATABASE SAIL
END;
GO
	USE SAIL;
GO
	CREATE SCHEMA SAIL0000V;
GO
-- =============================================
-- || CREATE WDSD TABLE
-- =============================================
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'WDSD_MAIN' and xtype = 'U')
BEGIN
	CREATE TABLE SAIL0000V.WDSD_MAIN (
		alf varchar(36) not null, 
		wob DATE not null,
		gndr_cd INT,
		lsoa2011_cd varchar(20)
		)
END;
GO
-- =============================================
-- || CREATE WLGP_REG TABLE
-- =============================================
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'WLGP_REG' and xtype = 'U')
BEGIN
	CREATE TABLE SAIL0000V.WLGP_REG (
		alf varchar(36) not null,
		start_dt DATE not null,
		end_dt DATE not null
	)
END;
GO
-- =============================================
-- || CREATE WLGP_EVENT TABLE
-- =============================================
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'WLGP_EVENT' and xtype = 'U')
BEGIN
	CREATE TABLE SAIL0000V.WLGP_EVENT (
		alf varchar(36) not null,
		gndr_cd INT,
		wob DATE,
		event_dt DATE,
		event_cd varchar(36)
	)
END;
GO
-- =============================================
-- || CREATE PEDW_EPISODE TABLE
-- =============================================
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'PEDW_EPISODE' and xtype = 'U')
BEGIN
	CREATE TABLE SAIL0000V.PEDW_EPISODE (
		spell_num varchar(36),
		epi_num int,
		epi_start DATE,
		epi_end date,
		epi_duration int,
		first_epi_in_spell int
	)
END;
GO
-- =============================================
-- || CREATE PEDW_SPELL TABLE
-- =============================================
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'PEDW_SPELL' and xtype = 'U')
BEGIN
	CREATE TABLE SAIL0000V.PEDW_SPELL (
		spell_num varchar(36),
		gndr_cd int,
		admis_yr int,
		admis_dt date,
		disch_yr int,
		disch_date date,
		spell_duration int,
		alf varchar(36)
	)
END;
GO
-- =============================================
-- || CREATE PEDW_SUPERSPELL TABLE
-- =============================================
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'PEDW_SUPERSPELL' and xtype = 'U')
BEGIN
	CREATE TABLE SAIL0000V.PEDW_SUPERSPELL (
		person_spell_num varchar(36),
		provider_spell_num varchar(36),
		spell_num varchar(36),
		epi_num int
	)
END;
GO