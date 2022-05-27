# The objective of the china_dw database is to accomplish preliminary
# data cleaning while organizing the source data files into a star schema
# in order to facilitate further data manipulation.

CREATE DATABASE `china_dw` /*!40100 DEFAULT CHARACTER SET latin1 */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE china_dw;


CREATE TABLE province_facts
SELECT DISTINCT gb.N_pinyin as Province,
	gb.ï»¿C_gbcode as 'Province_Code'
FROM china_dev.gbcodes as gb
INNER JOIN china_dev.hos_epi
ON ï»¿C_gbcode = GB86MC;

SELECT * FROM china_dw.province_facts;
SELECT COUNT(*) FROM china_dw.province_facts;

# Note: Some institutions were recorded with a province code (GB code) that is
# not included in the GB code documentation. As a result, these institutions
# have been necessarily omitted from the data in use. 10240 vs 9993, 2.4% of 
# the original sample data was affected by this issue.
# down t 2155! lots of repeats.


CREATE TABLE dim_hos
SELECT gb.ï»¿C_gbcode as 'Province_Code',
	he.SPEC as 'Specialty_Code',
    he.YEARA as 'Year_Founded',
    he.BEDS as Beds,
    he.PERS as 'Total_Personnel',
    he.TECH as 'Specialized_Staff',
    he.DAYS as 'Patient_Days'
FROM china_dev.hos_epi as he
INNER JOIN china_dev.gbcodes as gb
ON GB86MC = ï»¿C_gbcode
WHERE he.TYPE = 1;

SELECT * FROM china_dw.dim_hos;
SELECT COUNT(*) FROM china_dw.dim_hos;


CREATE TABLE dim_epi
SELECT gb.ï»¿C_gbcode as 'Province_Code',
	he.SPEC as 'Specialty_Code',
    he.YEARA as 'Year_Founded',
    he.BEDS as Beds,
    he.PERS as 'Total_Personnel',
    he.TECH as 'Specialized_Staff',
    he.DAYS as 'Patient_Days'
FROM china_dev.hos_epi as he
INNER JOIN china_dev.gbcodes as gb
ON GB86MC = ï»¿C_gbcode
WHERE he.TYPE = 2;

SELECT * FROM china_dw.dim_epi;
SELECT COUNT(*) FROM china_dw.dim_epi;


CREATE TABLE dim_spec
SELECT sp.ï»¿code as 'Specialty_Code',
	sp.specialty as Specialty
FROM china_dev.specialty_codes as sp;

SELECT * FROM china_dw.dim_spec;
SELECT COUNT(*) FROM china_dw.dim_spec;