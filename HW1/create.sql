CREATE TABLE H (
	oid SERIAL NOT NULL
);
ALTER TABLE H ADD CONSTRAINT pk_h_oid PRIMARY KEY (oid);

CREATE TABLE V (
	oid INTEGER NOT NULL,
  "key" VARCHAR(8) NOT NULL,
  val VARCHAR(8) NOT NULL
);

ALTER TABLE V ADD CONSTRAINT fk_v_h FOREIGN KEY (oid) REFERENCES H (oid) ON DELETE CASCADE;

