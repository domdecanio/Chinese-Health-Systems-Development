# The objective of the etl_dw database is to store two manipulations of the
# data stored in the china_dw database. The two tables in this database, 
# etl_epi and etl_hos, store all data on epidemiological centers and hospitals
# from the source data, respectively.

CREATE DATABASE `etl_dw` /*!40100 DEFAULT CHARACTER SET latin1 */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE etl_dw;


CREATE TABLE etl_hos
SELECT pf.Province,
	s.Specialty,
    hos.Year_Founded,
    hos.Beds,
    hos.Total_Personnel,
    hos.Specialized_Staff,
    hos.Patient_Days
FROM china_dw.dim_hos as hos
INNER JOIN china_dw.province_facts as pf
ON hos.Province_Code = pf.Province_Code
INNER JOIN china_dw.dim_spec as s
ON hos.Specialty_Code = s.Specialty_Code;

SELECT * FROM etl_hos;
SELECT COUNT(*) FROM etl_hos;


CREATE TABLE etl_epi
SELECT pf.Province,
	s.Specialty,
    epi.Year_Founded,
    epi.Beds,
    epi.Total_Personnel,
    epi.Specialized_Staff,
    epi.Patient_Days
FROM china_dw.dim_epi as epi
INNER JOIN china_dw.province_facts as pf
ON epi.Province_Code = pf.Province_Code
INNER JOIN china_dw.dim_spec as s
ON epi.Specialty_Code = s.Specialty_Code;

SELECT * FROM etl_epi;
SELECT COUNT(*) FROM etl_epi;