CREATE TABLE regulatory_permits (
permit_id varchar (20) primary key,
conservation_id varchar (20),
permit_type varchar (100),
authority varchar (100),
approval_date date,
permit_status varchar (100)
)

CREATE TABLE land_tenure_records (
tenure_id varchar (20) primary key,
conservation_id varchar (20),
land_type varchar (50),
owner varchar (50),
legal_document varchar (50),
boundary_defined varchar (20)
)

CREATE TABLE biodiversity_monitoring (
bio_id varchar (20) primary key,
conservation_id varchar (20),
species_count integer,
tree_density integer,
water_quality varchar (50),
assesment_date date
)

COPY regulatory_permits FROM 'D:\ECO TECHNO LEADER\KONSERVASI-MANGROVE-BERBASIS-BLOCKCHAIN-main\010Regulatory_Permits.csv' DELIMITER ',' CSV HEADER;

SELECT * FROM regulatory_permits

COPY land_tenure_records FROM 'D:\ECO TECHNO LEADER\KONSERVASI-MANGROVE-BERBASIS-BLOCKCHAIN-main\011Land_Tenure_Records.csv' DELIMITER ',' CSV HEADER;

SELECT * FROM land_tenure_records

COPY biodiversity_monitoring FROM 'D:\ECO TECHNO LEADER\KONSERVASI-MANGROVE-BERBASIS-BLOCKCHAIN-main\012Biodiversity_Monitoring.csv' DELIMITER ',' CSV HEADER;


SELECT 
    rp.permit_status AS Status_Izin,
    -- bm.water_quality ,
    AVG (CASE
        WHEN bm.water_quality = 'Poor' THEN 1
        WHEN bm.water_quality = 'Moderate' THEN 2
        WHEN bm.water_quality = 'Good' THEN 3
    ELSE 0
    END) AS kualitas_air,
    -- bm.tree_density
    AVG(bm.tree_density) AS Rataan_Kerapatan_Pohon ,
    COUNT(*) AS Jumlah_Proyek
FROM regulatory_permits rp
JOIN land_tenure_records ltr ON rp.conservation_id = ltr.conservation_id
JOIN biodiversity_monitoring bm ON rp.conservation_id = bm.conservation_id
GROUP BY rp.permit_status
ORDER BY Rataan_Kerapatan_Pohon DESC

--Query 2

SELECT
    rp.permit_status,
    ltr.land_type,
    ltr.boundary_defined,
    bm.species_count,
    bm.tree_density,
  CASE
       WHEN bm.water_quality = 'Poor' THEN 1
       WHEN bm.water_quality = 'Moderate' THEN 2
       WHEN bm.water_quality = 'Good' THEN 3
    ELSE 0
    END AS kualitas_air
FROM regulatory_permits rp
JOIN land_tenure_records ltr ON rp.conservation_id = ltr.conservation_id
JOIN biodiversity_monitoring bm ON rp.conservation_id = bm.conservation_id