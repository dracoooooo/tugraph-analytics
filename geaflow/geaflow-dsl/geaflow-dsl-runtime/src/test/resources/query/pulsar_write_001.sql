CREATE TABLE tbl_result (
	user_name varchar,
	user_count bigint
) WITH (
	type='pulsar',
    `geaflow.dsl.pulsar.serviceurl` = 'pulsar://localhost:6650',
    `geaflow.dsl.pulsar.topic` = 'sink-test'
);

INSERT INTO tbl_result VALUES ('json', 111)
;
