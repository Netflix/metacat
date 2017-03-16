--
-- PostgreSQL database dump
--

SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = public, pg_catalog;

--
-- Name: actor_actor_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('actor_actor_id_seq', 200, true);


--
-- Name: category_category_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('category_category_id_seq', 16, true);


--
-- Name: film_film_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('film_film_id_seq', 1000, true);


--
-- Name: address_address_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('address_address_id_seq', 605, true);


--
-- Name: city_city_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('city_city_id_seq', 600, true);


--
-- Name: country_country_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('country_country_id_seq', 109, true);


--
-- Name: customer_customer_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('customer_customer_id_seq', 599, true);


--
-- Name: inventory_inventory_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('inventory_inventory_id_seq', 4581, true);


--
-- Name: language_language_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('language_language_id_seq', 6, true);


--
-- Name: payment_payment_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('payment_payment_id_seq', 32098, true);


--
-- Name: rental_rental_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('rental_rental_id_seq', 16049, true);


--
-- Name: staff_staff_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('staff_staff_id_seq', 2, true);


--
-- Name: store_store_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('store_store_id_seq', 2, true);


--
-- Data for Name: actor; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE actor DISABLE TRIGGER ALL;

INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (1, 'PENELOPE', 'GUINESS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (2, 'NICK', 'WAHLBERG', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (3, 'ED', 'CHASE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (4, 'JENNIFER', 'DAVIS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (5, 'JOHNNY', 'LOLLOBRIGIDA', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (6, 'BETTE', 'NICHOLSON', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (7, 'GRACE', 'MOSTEL', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (8, 'MATTHEW', 'JOHANSSON', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (9, 'JOE', 'SWANK', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (10, 'CHRISTIAN', 'GABLE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (11, 'ZERO', 'CAGE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (12, 'KARL', 'BERRY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (13, 'UMA', 'WOOD', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (14, 'VIVIEN', 'BERGEN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (15, 'CUBA', 'OLIVIER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (16, 'FRED', 'COSTNER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (17, 'HELEN', 'VOIGHT', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (18, 'DAN', 'TORN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (19, 'BOB', 'FAWCETT', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (20, 'LUCILLE', 'TRACY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (21, 'KIRSTEN', 'PALTROW', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (22, 'ELVIS', 'MARX', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (23, 'SANDRA', 'KILMER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (24, 'CAMERON', 'STREEP', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (25, 'KEVIN', 'BLOOM', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (26, 'RIP', 'CRAWFORD', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (27, 'JULIA', 'MCQUEEN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (28, 'WOODY', 'HOFFMAN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (29, 'ALEC', 'WAYNE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (30, 'SANDRA', 'PECK', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (31, 'SISSY', 'SOBIESKI', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (32, 'TIM', 'HACKMAN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (33, 'MILLA', 'PECK', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (34, 'AUDREY', 'OLIVIER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (35, 'JUDY', 'DEAN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (36, 'BURT', 'DUKAKIS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (37, 'VAL', 'BOLGER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (38, 'TOM', 'MCKELLEN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (39, 'GOLDIE', 'BRODY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (40, 'JOHNNY', 'CAGE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (41, 'JODIE', 'DEGENERES', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (42, 'TOM', 'MIRANDA', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (43, 'KIRK', 'JOVOVICH', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (44, 'NICK', 'STALLONE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (45, 'REESE', 'KILMER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (46, 'PARKER', 'GOLDBERG', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (47, 'JULIA', 'BARRYMORE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (48, 'FRANCES', 'DAY-LEWIS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (49, 'ANNE', 'CRONYN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (50, 'NATALIE', 'HOPKINS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (51, 'GARY', 'PHOENIX', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (52, 'CARMEN', 'HUNT', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (53, 'MENA', 'TEMPLE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (54, 'PENELOPE', 'PINKETT', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (55, 'FAY', 'KILMER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (56, 'DAN', 'HARRIS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (57, 'JUDE', 'CRUISE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (58, 'CHRISTIAN', 'AKROYD', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (59, 'DUSTIN', 'TAUTOU', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (60, 'HENRY', 'BERRY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (61, 'CHRISTIAN', 'NEESON', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (62, 'JAYNE', 'NEESON', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (63, 'CAMERON', 'WRAY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (64, 'RAY', 'JOHANSSON', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (65, 'ANGELA', 'HUDSON', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (66, 'MARY', 'TANDY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (67, 'JESSICA', 'BAILEY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (68, 'RIP', 'WINSLET', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (69, 'KENNETH', 'PALTROW', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (70, 'MICHELLE', 'MCCONAUGHEY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (71, 'ADAM', 'GRANT', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (72, 'SEAN', 'WILLIAMS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (73, 'GARY', 'PENN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (74, 'MILLA', 'KEITEL', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (75, 'BURT', 'POSEY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (76, 'ANGELINA', 'ASTAIRE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (77, 'CARY', 'MCCONAUGHEY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (78, 'GROUCHO', 'SINATRA', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (79, 'MAE', 'HOFFMAN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (80, 'RALPH', 'CRUZ', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (81, 'SCARLETT', 'DAMON', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (82, 'WOODY', 'JOLIE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (83, 'BEN', 'WILLIS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (84, 'JAMES', 'PITT', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (85, 'MINNIE', 'ZELLWEGER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (86, 'GREG', 'CHAPLIN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (87, 'SPENCER', 'PECK', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (88, 'KENNETH', 'PESCI', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (89, 'CHARLIZE', 'DENCH', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (90, 'SEAN', 'GUINESS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (91, 'CHRISTOPHER', 'BERRY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (92, 'KIRSTEN', 'AKROYD', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (93, 'ELLEN', 'PRESLEY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (94, 'KENNETH', 'TORN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (95, 'DARYL', 'WAHLBERG', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (96, 'GENE', 'WILLIS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (97, 'MEG', 'HAWKE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (98, 'CHRIS', 'BRIDGES', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (99, 'JIM', 'MOSTEL', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (100, 'SPENCER', 'DEPP', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (101, 'SUSAN', 'DAVIS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (102, 'WALTER', 'TORN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (103, 'MATTHEW', 'LEIGH', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (104, 'PENELOPE', 'CRONYN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (105, 'SIDNEY', 'CROWE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (106, 'GROUCHO', 'DUNST', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (107, 'GINA', 'DEGENERES', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (108, 'WARREN', 'NOLTE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (109, 'SYLVESTER', 'DERN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (110, 'SUSAN', 'DAVIS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (111, 'CAMERON', 'ZELLWEGER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (112, 'RUSSELL', 'BACALL', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (113, 'MORGAN', 'HOPKINS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (114, 'MORGAN', 'MCDORMAND', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (115, 'HARRISON', 'BALE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (116, 'DAN', 'STREEP', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (117, 'RENEE', 'TRACY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (118, 'CUBA', 'ALLEN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (119, 'WARREN', 'JACKMAN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (120, 'PENELOPE', 'MONROE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (121, 'LIZA', 'BERGMAN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (122, 'SALMA', 'NOLTE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (123, 'JULIANNE', 'DENCH', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (124, 'SCARLETT', 'BENING', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (125, 'ALBERT', 'NOLTE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (126, 'FRANCES', 'TOMEI', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (127, 'KEVIN', 'GARLAND', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (128, 'CATE', 'MCQUEEN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (129, 'DARYL', 'CRAWFORD', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (130, 'GRETA', 'KEITEL', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (131, 'JANE', 'JACKMAN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (132, 'ADAM', 'HOPPER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (133, 'RICHARD', 'PENN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (134, 'GENE', 'HOPKINS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (135, 'RITA', 'REYNOLDS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (136, 'ED', 'MANSFIELD', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (137, 'MORGAN', 'WILLIAMS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (138, 'LUCILLE', 'DEE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (139, 'EWAN', 'GOODING', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (140, 'WHOOPI', 'HURT', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (141, 'CATE', 'HARRIS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (142, 'JADA', 'RYDER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (143, 'RIVER', 'DEAN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (144, 'ANGELA', 'WITHERSPOON', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (145, 'KIM', 'ALLEN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (146, 'ALBERT', 'JOHANSSON', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (147, 'FAY', 'WINSLET', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (148, 'EMILY', 'DEE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (149, 'RUSSELL', 'TEMPLE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (150, 'JAYNE', 'NOLTE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (151, 'GEOFFREY', 'HESTON', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (152, 'BEN', 'HARRIS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (153, 'MINNIE', 'KILMER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (154, 'MERYL', 'GIBSON', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (155, 'IAN', 'TANDY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (156, 'FAY', 'WOOD', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (157, 'GRETA', 'MALDEN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (158, 'VIVIEN', 'BASINGER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (159, 'LAURA', 'BRODY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (160, 'CHRIS', 'DEPP', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (161, 'HARVEY', 'HOPE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (162, 'OPRAH', 'KILMER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (163, 'CHRISTOPHER', 'WEST', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (164, 'HUMPHREY', 'WILLIS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (165, 'AL', 'GARLAND', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (166, 'NICK', 'DEGENERES', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (167, 'LAURENCE', 'BULLOCK', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (168, 'WILL', 'WILSON', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (169, 'KENNETH', 'HOFFMAN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (170, 'MENA', 'HOPPER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (171, 'OLYMPIA', 'PFEIFFER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (172, 'GROUCHO', 'WILLIAMS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (173, 'ALAN', 'DREYFUSS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (174, 'MICHAEL', 'BENING', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (175, 'WILLIAM', 'HACKMAN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (176, 'JON', 'CHASE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (177, 'GENE', 'MCKELLEN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (178, 'LISA', 'MONROE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (179, 'ED', 'GUINESS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (180, 'JEFF', 'SILVERSTONE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (181, 'MATTHEW', 'CARREY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (182, 'DEBBIE', 'AKROYD', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (183, 'RUSSELL', 'CLOSE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (184, 'HUMPHREY', 'GARLAND', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (185, 'MICHAEL', 'BOLGER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (186, 'JULIA', 'ZELLWEGER', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (187, 'RENEE', 'BALL', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (188, 'ROCK', 'DUKAKIS', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (189, 'CUBA', 'BIRCH', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (190, 'AUDREY', 'BAILEY', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (191, 'GREGORY', 'GOODING', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (192, 'JOHN', 'SUVARI', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (193, 'BURT', 'TEMPLE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (194, 'MERYL', 'ALLEN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (195, 'JAYNE', 'SILVERSTONE', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (196, 'BELA', 'WALKEN', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (197, 'REESE', 'WEST', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (198, 'MARY', 'KEITEL', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (199, 'JULIA', 'FAWCETT', '2006-02-15 09:34:33');
INSERT INTO actor (actor_id, first_name, last_name, last_update) VALUES (200, 'THORA', 'TEMPLE', '2006-02-15 09:34:33');


ALTER TABLE actor ENABLE TRIGGER ALL;

--
-- Data for Name: address; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE address DISABLE TRIGGER ALL;



ALTER TABLE address ENABLE TRIGGER ALL;

--
-- Data for Name: category; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE category DISABLE TRIGGER ALL;

INSERT INTO category (category_id, name, last_update) VALUES (1, 'Action', '2006-02-15 09:46:27');
INSERT INTO category (category_id, name, last_update) VALUES (2, 'Animation', '2006-02-15 09:46:27');
INSERT INTO category (category_id, name, last_update) VALUES (3, 'Children', '2006-02-15 09:46:27');
INSERT INTO category (category_id, name, last_update) VALUES (4, 'Classics', '2006-02-15 09:46:27');
INSERT INTO category (category_id, name, last_update) VALUES (5, 'Comedy', '2006-02-15 09:46:27');
INSERT INTO category (category_id, name, last_update) VALUES (6, 'Documentary', '2006-02-15 09:46:27');
INSERT INTO category (category_id, name, last_update) VALUES (7, 'Drama', '2006-02-15 09:46:27');
INSERT INTO category (category_id, name, last_update) VALUES (8, 'Family', '2006-02-15 09:46:27');
INSERT INTO category (category_id, name, last_update) VALUES (9, 'Foreign', '2006-02-15 09:46:27');
INSERT INTO category (category_id, name, last_update) VALUES (10, 'Games', '2006-02-15 09:46:27');
INSERT INTO category (category_id, name, last_update) VALUES (11, 'Horror', '2006-02-15 09:46:27');
INSERT INTO category (category_id, name, last_update) VALUES (12, 'Music', '2006-02-15 09:46:27');
INSERT INTO category (category_id, name, last_update) VALUES (13, 'New', '2006-02-15 09:46:27');
INSERT INTO category (category_id, name, last_update) VALUES (14, 'Sci-Fi', '2006-02-15 09:46:27');
INSERT INTO category (category_id, name, last_update) VALUES (15, 'Sports', '2006-02-15 09:46:27');
INSERT INTO category (category_id, name, last_update) VALUES (16, 'Travel', '2006-02-15 09:46:27');


ALTER TABLE category ENABLE TRIGGER ALL;

--
-- Data for Name: city; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE city DISABLE TRIGGER ALL;



ALTER TABLE city ENABLE TRIGGER ALL;

--
-- Data for Name: country; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE country DISABLE TRIGGER ALL;

INSERT INTO country (country_id, country, last_update) VALUES (1, 'Afghanistan', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (2, 'Algeria', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (3, 'American Samoa', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (4, 'Angola', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (5, 'Anguilla', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (6, 'Argentina', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (7, 'Armenia', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (8, 'Australia', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (9, 'Austria', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (10, 'Azerbaijan', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (11, 'Bahrain', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (12, 'Bangladesh', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (13, 'Belarus', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (14, 'Bolivia', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (15, 'Brazil', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (16, 'Brunei', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (17, 'Bulgaria', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (18, 'Cambodia', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (19, 'Cameroon', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (20, 'Canada', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (21, 'Chad', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (22, 'Chile', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (23, 'China', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (24, 'Colombia', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (25, 'Congo, The Democratic Republic of the', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (26, 'Czech Republic', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (27, 'Dominican Republic', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (28, 'Ecuador', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (29, 'Egypt', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (30, 'Estonia', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (31, 'Ethiopia', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (32, 'Faroe Islands', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (33, 'Finland', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (34, 'France', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (35, 'French Guiana', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (36, 'French Polynesia', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (37, 'Gambia', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (38, 'Germany', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (39, 'Greece', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (40, 'Greenland', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (41, 'Holy See (Vatican City State)', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (42, 'Hong Kong', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (43, 'Hungary', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (44, 'India', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (45, 'Indonesia', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (46, 'Iran', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (47, 'Iraq', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (48, 'Israel', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (49, 'Italy', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (50, 'Japan', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (51, 'Kazakstan', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (52, 'Kenya', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (53, 'Kuwait', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (54, 'Latvia', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (55, 'Liechtenstein', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (56, 'Lithuania', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (57, 'Madagascar', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (58, 'Malawi', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (59, 'Malaysia', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (60, 'Mexico', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (61, 'Moldova', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (62, 'Morocco', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (63, 'Mozambique', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (64, 'Myanmar', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (65, 'Nauru', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (66, 'Nepal', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (67, 'Netherlands', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (68, 'New Zealand', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (69, 'Nigeria', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (70, 'North Korea', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (71, 'Oman', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (72, 'Pakistan', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (73, 'Paraguay', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (74, 'Peru', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (75, 'Philippines', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (76, 'Poland', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (77, 'Puerto Rico', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (78, 'Romania', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (79, 'Runion', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (80, 'Russian Federation', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (81, 'Saint Vincent and the Grenadines', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (82, 'Saudi Arabia', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (83, 'Senegal', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (84, 'Slovakia', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (85, 'South Africa', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (86, 'South Korea', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (87, 'Spain', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (88, 'Sri Lanka', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (89, 'Sudan', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (90, 'Sweden', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (91, 'Switzerland', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (92, 'Taiwan', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (93, 'Tanzania', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (94, 'Thailand', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (95, 'Tonga', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (96, 'Tunisia', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (97, 'Turkey', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (98, 'Turkmenistan', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (99, 'Tuvalu', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (100, 'Ukraine', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (101, 'United Arab Emirates', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (102, 'United Kingdom', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (103, 'United States', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (104, 'Venezuela', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (105, 'Vietnam', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (106, 'Virgin Islands, U.S.', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (107, 'Yemen', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (108, 'Yugoslavia', '2006-02-15 09:44:00');
INSERT INTO country (country_id, country, last_update) VALUES (109, 'Zambia', '2006-02-15 09:44:00');


ALTER TABLE country ENABLE TRIGGER ALL;

--
-- Data for Name: customer; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE customer DISABLE TRIGGER ALL;



ALTER TABLE customer ENABLE TRIGGER ALL;

--
-- Data for Name: film; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE film DISABLE TRIGGER ALL;



ALTER TABLE film ENABLE TRIGGER ALL;

--
-- Data for Name: film_actor; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE film_actor DISABLE TRIGGER ALL;



ALTER TABLE film_actor ENABLE TRIGGER ALL;

--
-- Data for Name: film_category; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE film_category DISABLE TRIGGER ALL;



ALTER TABLE film_category ENABLE TRIGGER ALL;

--
-- Data for Name: inventory; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE inventory DISABLE TRIGGER ALL;



ALTER TABLE inventory ENABLE TRIGGER ALL;

--
-- Data for Name: language; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE language DISABLE TRIGGER ALL;



ALTER TABLE language ENABLE TRIGGER ALL;

--
-- Data for Name: payment; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE payment DISABLE TRIGGER ALL;



ALTER TABLE payment ENABLE TRIGGER ALL;

--
-- Data for Name: payment_p2007_01; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE payment_p2007_01 DISABLE TRIGGER ALL;



ALTER TABLE payment_p2007_01 ENABLE TRIGGER ALL;

--
-- Data for Name: payment_p2007_02; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE payment_p2007_02 DISABLE TRIGGER ALL;



ALTER TABLE payment_p2007_02 ENABLE TRIGGER ALL;

--
-- Data for Name: payment_p2007_03; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE payment_p2007_03 DISABLE TRIGGER ALL;



ALTER TABLE payment_p2007_03 ENABLE TRIGGER ALL;

--
-- Data for Name: payment_p2007_04; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE payment_p2007_04 DISABLE TRIGGER ALL;



ALTER TABLE payment_p2007_04 ENABLE TRIGGER ALL;

--
-- Data for Name: payment_p2007_05; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE payment_p2007_05 DISABLE TRIGGER ALL;



ALTER TABLE payment_p2007_05 ENABLE TRIGGER ALL;

--
-- Data for Name: payment_p2007_06; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE payment_p2007_06 DISABLE TRIGGER ALL;



ALTER TABLE payment_p2007_06 ENABLE TRIGGER ALL;

--
-- Data for Name: rental; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE rental DISABLE TRIGGER ALL;



ALTER TABLE rental ENABLE TRIGGER ALL;

--
-- Data for Name: staff; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE staff DISABLE TRIGGER ALL;



ALTER TABLE staff ENABLE TRIGGER ALL;

--
-- Data for Name: store; Type: TABLE DATA; Schema: public; Owner: postgres
--

ALTER TABLE store DISABLE TRIGGER ALL;



ALTER TABLE store ENABLE TRIGGER ALL;

--
-- PostgreSQL database dump complete
--

