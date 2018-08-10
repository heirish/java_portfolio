create table if not exists pattern_nodes(
project_id int(5) not null,
pattern_level int(5) not null,
pattern_key varchar(32) not null,
parent_key varchar(32) not null,
pattern varchar(4096) not null,
represent varchar(5000),
CONSTRAINT PK_pattern_nodes PRIMARY KEY (project_id, pattern_level, pattern_key)
);

create table if not exists projects(
id int(5) not null PRIMARY KEY,
project_name varchar(32) not null
);
