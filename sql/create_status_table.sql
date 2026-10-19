CREATE TABLE IF NOT EXISTS status (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(50) NOT NULL UNIQUE,
    descricao TEXT
);

INSERT INTO status (id, nome, descricao) VALUES
    (1, 'ATENDIDA', 'Solicitação atendida'),
    (2, 'REJEITADA', 'Solicitação rejeitada'),
    (3, 'PENDENTE', 'Solicitação pendente')
ON CONFLICT (id) DO UPDATE SET
    nome = EXCLUDED.nome,
    descricao = EXCLUDED.descricao;

ALTER TABLE nup
    ADD CONSTRAINT fk_nup_status
    FOREIGN KEY (status)
    REFERENCES status (id);
