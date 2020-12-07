--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.2
-- Dumped by pg_dump version 9.6.2

-- Started on 2020-11-04 14:02:05

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 1 (class 3079 OID 12387)
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- TOC entry 2130 (class 0 OID 0)
-- Dependencies: 1
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 186 (class 1259 OID 1235748)
-- Name: tes; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE tes (
    id integer NOT NULL,
    nama text,
    umur integer
);


ALTER TABLE tes OWNER TO postgres;

--
-- TOC entry 185 (class 1259 OID 1235746)
-- Name: tes_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE tes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE tes_id_seq OWNER TO postgres;

--
-- TOC entry 2131 (class 0 OID 0)
-- Dependencies: 185
-- Name: tes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE tes_id_seq OWNED BY tes.id;


--
-- TOC entry 2002 (class 2604 OID 1235751)
-- Name: tes id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY tes ALTER COLUMN id SET DEFAULT nextval('tes_id_seq'::regclass);


--
-- TOC entry 2123 (class 0 OID 1235748)
-- Dependencies: 186
-- Data for Name: tes; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY tes (id, nama, umur) FROM stdin;
2	nama2	34
1	nama1	55
\.


--
-- TOC entry 2132 (class 0 OID 0)
-- Dependencies: 185
-- Name: tes_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('tes_id_seq', 2, true);


--
-- TOC entry 2004 (class 2606 OID 1235756)
-- Name: tes tes_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY tes
    ADD CONSTRAINT tes_pkey PRIMARY KEY (id);


-- Completed on 2020-11-04 14:02:07

--
-- PostgreSQL database dump complete
--

