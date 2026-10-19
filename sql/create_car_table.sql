CREATE TABLE IF NOT EXISTS car (
    car_id SERIAL PRIMARY KEY,
    nup_id INTEGER,
    cod_imovel VARCHAR(100) UNIQUE,
    status_imo VARCHAR(2),
    dat_criaca DATE,
    data_atual DATE,
    area DOUBLE PRECISION,
    condicao VARCHAR(100),
    uf VARCHAR(2),
    municipio VARCHAR(100),
    cod_munici INTEGER,
    m_fiscal DOUBLE PRECISION,
    tipo_imove VARCHAR(10),
    geom GEOMETRY(MultiPolygon, 4674),
    CONSTRAINT fk_car_nup FOREIGN KEY (nup_id) REFERENCES nup (id)
);

CREATE INDEX IF NOT EXISTS idx_car_cod_imovel ON car (cod_imovel);
CREATE INDEX IF NOT EXISTS idx_car_nup_id ON car (nup_id);
