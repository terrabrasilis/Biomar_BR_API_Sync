CREATE TABLE IF NOT EXISTS nup_status (
    status INTEGER PRIMARY KEY,
    description TEXT NOT NULL
);

INSERT INTO nup_status (status, description) VALUES
    (1, 'ATENDIDA'),
    (2, 'REJEITADA'),
    (3, 'PENDENTE'),
    (4, 'AGUARDANDO'),
    (5, 'EM ATENDIMENTO')
ON CONFLICT (status) DO UPDATE SET
    description = EXCLUDED.description;

ALTER TABLE nup RENAME COLUMN status TO nup_status_id;

ALTER TABLE nup
    ADD CONSTRAINT fk_nup_nup_status
    FOREIGN KEY (nup_status_id)
    REFERENCES nup_status (status);
