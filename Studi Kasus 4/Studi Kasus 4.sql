CREATE TABLE regulatory_permits (
    permit_id VARCHAR(10) PRIMARY KEY,
    conservation_id VARCHAR(10),
    permit_type VARCHAR(50),
    authority VARCHAR(50),
    approval_date DATE,
    permit_status VARCHAR(20) CHECK (permit_status IN ('Approved', 'Pending'))
);

CREATE TABLE mangrove_conservation_records (
    conservation_id VARCHAR(10) PRIMARY KEY,
    location VARCHAR(50) NOT NULL,
    area_ha INTEGER CHECK (area_ha > 0),
    carbon_credits INTEGER CHECK (carbon_credits >= 0),
    date_recorded DATE NOT NULL
);

CREATE TABLE land_tenure_records (
    tenure_id VARCHAR(10) PRIMARY KEY,
    conservation_id VARCHAR(10),
    land_type VARCHAR(50),
    owner VARCHAR(50),
    legal_document VARCHAR(50),
    boundary_defined VARCHAR(3) CHECK (boundary_defined IN ('Yes', 'No'))
);

CREATE TABLE biodiversity_monitoring (
    bio_id VARCHAR(10) PRIMARY KEY,
    conservation_id VARCHAR(10),
    species_count INTEGER CHECK (species_count >= 0),
    tree_density INTEGER CHECK (tree_density >= 0),
    water_quality VARCHAR(20) CHECK (water_quality IN ('Good', 'Moderate', 'Poor')),
    assessment_date DATE NOT NULL
);

-- Tabel conservation_activities
CREATE TABLE conservation_activities (
    activity_id VARCHAR(10) PRIMARY KEY,
    conservation_id VARCHAR(10) ,
    activity_type VARCHAR(20) CHECK (activity_type IN ('Planting', 'Monitoring', 'Restoration')),
    date_performed DATE NOT NULL,
    participants INTEGER CHECK (participants >= 0)
);

CREATE TABLE blockchain_data_compliance (
data_id varchar (20) primary key,
conservation_id varchar (20),
data_type varchar (50),
consent_obtained varchar (20),
encryption_level varchar (20),
access_level varchar (20)
)

CREATE TABLE titik_koordinat_kabupaten (
    conservation_id VARCHAR(10) PRIMARY KEY,
    location VARCHAR(50) NOT NULL,
    area_ha INTEGER CHECK (area_ha > 0),
    carbon_credits INTEGER CHECK (carbon_credits >= 0),
    date_recorded DATE NOT NULL,
    latitude NUMERIC(9,4),
    longitude NUMERIC(9,4)
);

COPY titik_koordinat_kabupaten
FROM 'D:\ECO TECHNO LEADER\KONSERVASI-MANGROVE-BERBASIS-BLOCKCHAIN-main/titik_koordinat_kabupaten.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY regulatory_permits 
FROM 'D:\ECO TECHNO LEADER\KONSERVASI-MANGROVE-BERBASIS-BLOCKCHAIN-main/010Regulatory_Permits.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',');
COPY land_tenure_records 
FROM 'D:\ECO TECHNO LEADER\KONSERVASI-MANGROVE-BERBASIS-BLOCKCHAIN-main/011Land_Tenure_Records.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',');
COPY biodiversity_monitoring 
FROM 'D:\ECO TECHNO LEADER\KONSERVASI-MANGROVE-BERBASIS-BLOCKCHAIN-main/012Biodiversity_Monitoring.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',');
COPY conservation_activities 
FROM 'D:\ECO TECHNO LEADER\KONSERVASI-MANGROVE-BERBASIS-BLOCKCHAIN-main/006Conservation_Activities.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY mangrove_conservation_records 
FROM 'D:\ECO TECHNO LEADER\KONSERVASI-MANGROVE-BERBASIS-BLOCKCHAIN-main/001Mangrove_Conservation_Records.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',');

COPY blockchain_data_compliance
FROM 'D:\ECO TECHNO LEADER\KONSERVASI-MANGROVE-BERBASIS-BLOCKCHAIN-main\013Blockchain_Data_Compliance.csv' DELIMITER ',' CSV HEADER;
SELECT * FROM blockchain_data_compliance

---analysis

WITH risk_scoring AS (
    SELECT 
        c.conservation_id,
        c.location,
        CASE 
            WHEN rp.permit_status = 'Pending' AND lt.boundary_defined = 'No' THEN 30 
            ELSE 0 
        END AS risiko_izin,
        CASE 
            WHEN lt.land_type = 'Community Land' AND bd.access_level = 'Restricted' THEN 40 
            ELSE 0 
        END AS risiko_masyarakat,
        CASE 
            WHEN bm.water_quality = 'Poor' AND ca.activity_type = 'Restoration' THEN 30 
            ELSE 0 
        END AS risiko_kualitas_air
    FROM mangrove_conservation_records c
    JOIN regulatory_permits rp ON c.conservation_id = rp.conservation_id
    JOIN land_tenure_records lt ON c.conservation_id = lt.conservation_id
    JOIN blockchain_data_compliance bd ON c.conservation_id = bd.conservation_id
    JOIN biodiversity_monitoring bm ON c.conservation_id = bm.conservation_id
    JOIN conservation_activities ca ON c.conservation_id = ca.conservation_id
    WHERE c.conservation_id IN (
        'C001', 'C002', 'C003', 'C004', 'C005', 'C006', 'C007', 'C008', 'C009', 'C010',
        'C011', 'C012', 'C013', 'C014', 'C015', 'C016', 'C017', 'C018', 'C019', 'C020'
    )
),

scored AS (
    SELECT 
        rs.*,
        (risiko_izin + risiko_masyarakat + risiko_kualitas_air) AS total_risiko,
        CASE 
            WHEN (risiko_izin + risiko_masyarakat + risiko_kualitas_air) >= 70 THEN 'Tinggi'
            WHEN (risiko_izin + risiko_masyarakat + risiko_kualitas_air) >= 30 THEN 'Sedang'
            ELSE 'Rendah'
        END AS tingkat_risiko
    FROM risk_scoring rs
)

SELECT 
    s.conservation_id,
    s.location,
    s.risiko_izin,
    s.risiko_masyarakat,
    s.risiko_kualitas_air,
    s.total_risiko,
    s.tingkat_risiko,
    k.latitude,
    k.longitude
FROM scored s
JOIN titik_koordinat_kabupaten k ON s.location = k.location
ORDER BY s.total_risiko DESC;


WITH risk_scoring AS (
    SELECT 
        c.conservation_id,
        c.location,
        CASE 
            WHEN rp.permit_status = 'Pending' AND lt.boundary_defined = 'No' THEN 30 
            ELSE 0 
        END AS risiko_izin,
        CASE 
            WHEN lt.land_type = 'Community Land' AND bd.access_level = 'Restricted' THEN 40 
            ELSE 0 
        END AS risiko_masyarakat,
        CASE 
            WHEN bm.water_quality = 'Poor' AND ca.activity_type = 'Restoration' THEN 30 
            ELSE 0 
        END AS risiko_kualitas_air
    FROM mangrove_conservation_records c
    JOIN regulatory_permits rp ON c.conservation_id = rp.conservation_id
    JOIN land_tenure_records lt ON c.conservation_id = lt.conservation_id
    JOIN blockchain_data_compliance bd ON c.conservation_id = bd.conservation_id
    JOIN biodiversity_monitoring bm ON c.conservation_id = bm.conservation_id
    JOIN conservation_activities ca ON c.conservation_id = ca.conservation_id
    WHERE c.conservation_id IN (
        'C001', 'C002', 'C003', 'C004', 'C005', 'C006', 'C007', 'C008', 'C009', 'C010',
        'C011', 'C012', 'C013', 'C014', 'C015', 'C016', 'C017', 'C018', 'C019', 'C020'
    )
),

scored AS (
    SELECT 
        rs.*,
        (risiko_izin + risiko_masyarakat + risiko_kualitas_air) AS total_risiko,
        CASE 
            WHEN (risiko_izin + risiko_masyarakat + risiko_kualitas_air) >= 70 THEN 'Tinggi'
            WHEN (risiko_izin + risiko_masyarakat + risiko_kualitas_air) >= 30 THEN 'Sedang'
            ELSE 'Rendah'
        END AS tingkat_risiko
    FROM risk_scoring rs
)

SELECT 
    s.conservation_id,
    s.location,
    s.risiko_izin,
    s.risiko_masyarakat,
    s.risiko_kualitas_air,
    s.total_risiko,
    s.tingkat_risiko,
    k.latitude,
    k.longitude,
    k.area_ha,
    k.carbon_credits,
    ROUND(
        CASE 
            WHEN k.area_ha > 0 THEN k.carbon_credits::NUMERIC / k.area_ha
            ELSE NULL
        END, 
        2
    ) AS risk_score
FROM scored s
JOIN titik_koordinat_kabupaten k ON s.location = k.location
ORDER BY s.total_risiko DESC;
