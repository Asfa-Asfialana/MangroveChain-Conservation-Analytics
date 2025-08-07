-- Membuat tabel Mangrove_Conservation_Records
CREATE TABLE mangrove_conservation_records (
    conservation_id VARCHAR(10) PRIMARY KEY,
    location VARCHAR(50) NOT NULL,
    area_ha INTEGER CHECK (area_ha > 0),
    carbon_credits INTEGER CHECK (carbon_credits >= 0),
    date_recorded DATE NOT NULL
);

-- Membuat tabel Community_Members
CREATE TABLE community_members (
    member_id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    role VARCHAR(20) CHECK (role IN ('Farmer', 'Fisherman', 'Activist')),
    contact_number VARCHAR(15) NOT NULL,
    join_date DATE NOT NULL
);

-- Membuat tabel Community_Engagement
CREATE TABLE community_engagement (
    engage_id VARCHAR(10) PRIMARY KEY,
    conservation_id VARCHAR(10) REFERENCES mangrove_conservation_records(conservation_id),
    activity_type VARCHAR(20) CHECK (activity_type IN ('Workshop', 'Consultation', 'Training')),
    participants INTEGER CHECK (participants > 0),
    benefit_distributed NUMERIC CHECK (benefit_distributed >= 0),
    engagement_date DATE NOT NULL
);

-- Impor data ke mangrove_conservation_records
COPY mangrove_conservation_records 
FROM 'D:\ECO TECHNO LEADER\KONSERVASI-MANGROVE-BERBASIS-BLOCKCHAIN-main\001Mangrove_Conservation_Records.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',');

-- Impor data ke community_members
COPY community_members 
FROM 'D:\ECO TECHNO LEADER\KONSERVASI-MANGROVE-BERBASIS-BLOCKCHAIN-main\004Community_Members.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',');

ALTER TABLE community_members
ADD COLUMN conservation_id TEXT;

WITH numbered_rows AS (
  SELECT ctid, ROW_NUMBER() OVER (ORDER BY ctid) AS rn
  FROM community_members
)
UPDATE community_members
SET conservation_id = 'C' || LPAD(rn::text, 3, '0')
FROM numbered_rows
WHERE community_members.ctid = numbered_rows.ctid
  AND numbered_rows.rn <= 30;

SELECT *
FROM community_members

-- Impor data ke community_engagement
COPY community_engagement 
FROM 'D:\ECO TECHNO LEADER\KONSERVASI-MANGROVE-BERBASIS-BLOCKCHAIN-main\014Community_Engagement.csv' 
WITH (FORMAT csv, HEADER true, DELIMITER ',');

SELECT *
FROM community_engagement

INSERT INTO mangrove_conservation_records (conservation_id, location, date_recorded)
VALUES 
  ('C021', 'Dummy Location', CURRENT_DATE),
  ('C022', 'Dummy Location', CURRENT_DATE),
  ('C023', 'Dummy Location', CURRENT_DATE),
  ('C024', 'Dummy Location', CURRENT_DATE),
  ('C025', 'Dummy Location', CURRENT_DATE),
  ('C026', 'Dummy Location', CURRENT_DATE),
  ('C027', 'Dummy Location', CURRENT_DATE),
  ('C028', 'Dummy Location', CURRENT_DATE),
  ('C029', 'Dummy Location', CURRENT_DATE),
  ('C030', 'Dummy Location', CURRENT_DATE);

SELECT *
FROM mangrove_conservation_records


---analysis 1
WITH partisipasi_normal AS (
    SELECT 
        ce.conservation_id,
        ce.participants::FLOAT / NULLIF((SELECT AVG(participants::FLOAT) FROM community_engagement), 0) AS partisipasi_relatif,
        COUNT(*) OVER (PARTITION BY ce.conservation_id) AS frekuensi_kegiatan,
        SUM(ce.benefit_distributed::NUMERIC) OVER (PARTITION BY ce.conservation_id) AS total_manfaat,
        ce.engagement_date,
        TO_CHAR(ce.engagement_date, 'YYYY-MM') AS month
    FROM community_engagement ce
    WHERE ce.conservation_id IN (
        SELECT conservation_id FROM mangrove_conservation_records
    )
),
biodiversitas_summary AS (
    SELECT
        mc.conservation_id,
        mc.area_ha,
        mc.carbon_credits,
        mc.date_recorded
    FROM mangrove_conservation_records mc
),
monthly_aggregation AS (
    SELECT
        pn.month,
        AVG(pn.partisipasi_relatif) AS avg_partisipasi_relatif,
        SUM(pn.frekuensi_kegiatan) AS total_frekuensi_kegiatan,
        SUM(pn.total_manfaat) AS total_manfaat_bulanan,
        AVG(bs.carbon_credits)::INTEGER AS avg_carbon_credits
    FROM partisipasi_normal pn
    LEFT JOIN biodiversitas_summary bs ON pn.conservation_id = bs.conservation_id
    GROUP BY pn.month
)
SELECT 
    month,
    avg_partisipasi_relatif,
    total_frekuensi_kegiatan,
    total_manfaat_bulanan,
    avg_carbon_credits
FROM monthly_aggregation
ORDER BY month;
-- Query yang dijalankan

WITH partisipasi_normal AS ( 
    SELECT 
        ce.Conservation_ID, 
        ce.Participants::INTEGER / NULLIF((SELECT AVG(Participants::INTEGER) FROM community_engagement), 0) AS partisipasi_relatif, 
        COUNT(*) OVER (PARTITION BY ce.Conservation_ID) AS frekuensi_kegiatan, 
        SUM(Benefit_Distributed::INTEGER) OVER (PARTITION BY ce.Conservation_ID) AS total_manfaat,
        ce.Engagement_Date
    FROM community_engagement ce
),
biodiversitas_summary AS (
    SELECT
        mc.Conservation_ID,
        mc.Area_Ha,
        mc.Carbon_Credits,
        mc.Date_Recorded
    FROM mangrove_conservation_records mc
)
SELECT 
    pn.Conservation_ID,
    pn.partisipasi_relatif,
    pn.frekuensi_kegiatan,
    pn.total_manfaat,
    pn.Engagement_Date,
    bs.Area_Ha,
    bs.Carbon_Credits,
    bs.Date_Recorded AS biodiversity_record_date
FROM partisipasi_normal pn
LEFT JOIN biodiversitas_summary bs ON pn.Conservation_ID = bs.Conservation_ID
ORDER BY pn.Engagement_Date;