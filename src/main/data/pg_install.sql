SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET search_path = public, pg_catalog;
SET default_tablespace = '';
SET default_with_oids = false;

DROP TABLE IF EXISTS public."@datasets";

CREATE TABLE "@datasets" (
    id bigint NOT NULL,
    key character varying(64),
    namespace character varying(64),
    layer integer,
    features text
);

--
-- Name: @datasets_id_seq; Type: SEQUENCE; Schema: public; Owner: rabbitpt
--

CREATE SEQUENCE "@datasets_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE "@datasets_id_seq" OWNED BY "@datasets".id;
ALTER TABLE ONLY "@datasets" ALTER COLUMN id SET DEFAULT nextval('"@datasets_id_seq"'::regclass);
-- ALTER TABLE "@datasets" DROP CONSTRAINT IF EXISTS "@datasets_key_key";
-- ALTER TABLE ONLY "@datasets" ADD CONSTRAINT "@datasets_key_key" UNIQUE (key);
--ALTER TABLE "@datasets" DROP CONSTRAINT IF EXISTS "@datasets_namepsace_key";
--ALTER TABLE ONLY "@datasets" ADD CONSTRAINT "@datasets_namepsace_key" UNIQUE (namepsace);
--ALTER TABLE "@datasets" DROP CONSTRAINT IF EXISTS "@datasets_pkey";
--ALTER TABLE ONLY "@datasets" ADD CONSTRAINT "@datasets_pkey" PRIMARY KEY (id);

REVOKE ALL ON TABLE "@datasets" FROM PUBLIC;
REVOKE ALL ON TABLE "@datasets" FROM rabbitpt;
GRANT ALL ON TABLE "@datasets" TO rabbitpt;
GRANT ALL ON TABLE "@datasets" TO rabbitpt_unn_datacenter;

DROP TABLE IF EXISTS public."@dependencies";

CREATE TABLE "@dependencies" (
    upstream character varying(64),
    downstream character varying(64)
);

REVOKE ALL ON TABLE "@dependencies" FROM PUBLIC;
REVOKE ALL ON TABLE "@dependencies" FROM rabbitpt;
GRANT ALL ON TABLE "@dependencies" TO rabbitpt;
GRANT ALL ON TABLE "@dependencies" TO rabbitpt_unn_datacenter;
