DROP TABLE IF EXISTS rayon;

DROP TABLE IF EXISTS xx1C;

DROP TABLE IF EXISTS xxCounter;

CREATE TABLE IF NOT EXISTS rayon (
    id INTEGER,
    name TEXT,
    turg INTEGER
);

CREATE TABLE IF NOT EXISTS xx1C (
    id TEXT,
    obj_conn TEXT,
    distr_conn TEXT,
    consumer TEXT,
    distr_consum TEXT,
    identifier TEXT,
    indications TEXT,
    date_indic TEXT,
    counter_numb Text
);

CREATE TABLE IF NOT EXISTS xxCounter (
    gas_meter_id TEXT,
    channel_id TEXT,
    fabric_num TEXT,
    gas_meter_model TEXT,
    type_name TEXT,
    pc_id TEXT,
    pc_name TEXT,
    orig_id TEXT,
    id_1c TEXT
);

INSERT INTO rayon (id,name,turg) VALUES
    (1,'Краснояружский',4),
    (2,'Грайворонский',4),
    (3,'Ивнянски',8),
    (4,'Ракитянский',4),
    (5,'Борисовский',4),
    (6,'Прохоровский',8),
    (7,'Яковлевский',8),
    (8,'Белгородский',3),
    (9,'Губкинский',7),
    (10,'Корочанский',9),
    (11,'Шебекинский',9),
    (12,'Старооскольский',7),
    (13,'Чернянский',6),
    (14,'Новооскольский',6),
    (15,'Волоконовский',6),
    (16,'Валуйский',5),
    (17,'Красненский',1),
    (18,'Красногвардейский',1),
    (19,'Алексеевский',1),
    (20,'Вейделевский',5),
    (21,'Ровеньский',5),
    (22,'Белгород',2);