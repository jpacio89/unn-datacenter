SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET search_path = public, pg_catalog;
SET default_tablespace = '';
SET default_with_oids = false;

CREATE TABLE "@datasets" (
    id bigint NOT NULL,
    key character varying(32),
    namepsace character varying(32),
    layer integer,
    features text
);

ALTER TABLE public."@datasets" OWNER TO rabbitpt;

--
-- Name: @datasets_id_seq; Type: SEQUENCE; Schema: public; Owner: rabbitpt
--

CREATE SEQUENCE "@datasets_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE public."@datasets_id_seq" OWNER TO rabbitpt;

ALTER SEQUENCE "@datasets_id_seq" OWNED BY "@datasets".id;

ALTER TABLE ONLY "@datasets" ALTER COLUMN id SET DEFAULT nextval('"@datasets_id_seq"'::regclass);

ALTER TABLE ONLY "@datasets"
    ADD CONSTRAINT "@datasets_key_key" UNIQUE (key);

ALTER TABLE ONLY "@datasets"
    ADD CONSTRAINT "@datasets_namepsace_key" UNIQUE (namepsace);


ALTER TABLE ONLY "@datasets"
    ADD CONSTRAINT "@datasets_pkey" PRIMARY KEY (id);



