CREATE TABLE pulsar_source (
	user_name varchar,
	user_count bigint
) WITH (
	type='pulsar',
    `geaflow.dsl.pulsar.serviceurl` = 'pulsar://localhost:6650',
    `geaflow.dsl.pulsar.topic` = 'sink-test',
    `geaflow.dsl.pulsar.subscription` = 'source-test-sub'
);

CREATE TABLE tbl_result (
	user_name varchar,
	user_count bigint
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

INSERT INTO tbl_result
SELECT *
FROM pulsar_source;
