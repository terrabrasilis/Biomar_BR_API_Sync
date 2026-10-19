CREATE TABLE IF NOT EXISTS prodes (
    id SERIAL PRIMARY KEY,
    fid INTEGER,
    geom GEOMETRY(MultiPolygon, 4674),
    state VARCHAR(2),
    path_row VARCHAR(10),
    main_class VARCHAR(50),
    class_name VARCHAR(50),
    def_cloud VARCHAR(50),
    julian_day INTEGER,
    year INTEGER,
    area_km DOUBLE PRECISION,
    scene_id INTEGER,
    source VARCHAR(100),
    satellite VARCHAR(50),
    sensor VARCHAR(50),
    uuid UUID UNIQUE,
    image_date DATE,
    sub_class VARCHAR(50),
    pub_date DATE,
    nup_id INTEGER,
    decision_id INTEGER,
    CONSTRAINT fk_prodes_nup FOREIGN KEY (nup_id) REFERENCES nup (id)
);

CREATE INDEX IF NOT EXISTS idx_prodes_uuid ON prodes (uuid);
CREATE INDEX IF NOT EXISTS idx_prodes_nup_id ON prodes (nup_id);
