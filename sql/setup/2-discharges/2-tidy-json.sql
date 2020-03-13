-- 2nd, tidy the JSON so we can explode it easily
-- This means 
-- (i) Extracting the `entries` array
-- (ii) For each entry in that array, picking out the `key` and making it a primary key in the resulting JSON
-- (iii) For each entry in that array, picking out the `value` and making it the value in the resulting JSON
-- Note that we end up building 3 JSONs rather than one because the json_build_object function only seems to support 50 key/value pairs
drop materialized view if exists scratch.discharges_form_tidy_json cascade;
create materialized view scratch.discharges_form_tidy_json as (
    select
    uid,
    ingested_at,
    json_build_object(
		coalesce(	"data"->'entries'->	0	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	0	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	1	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	1	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	2	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	2	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	3	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	3	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	4	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	4	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	5	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	5	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	6	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	6	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	7	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	7	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	8	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	8	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	9	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	9	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	10	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	10	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	11	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	11	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	12	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	12	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	13	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	13	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	14	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	14	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	15	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	15	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	16	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	16	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	17	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	17	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	18	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	18	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	19	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	19	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	20	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	20	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	21	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	21	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	22	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	22	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	23	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	23	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	24	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	24	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	25	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	25	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	26	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	26	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	27	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	27	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	28	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	28	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	29	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	29	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	30	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	30	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	31	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	31	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	32	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	32	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	33	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	33	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	34	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	34	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	35	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	35	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	36	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	36	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	37	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	37	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	38	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	38	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	39	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	39	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	40	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	40	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	41	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	41	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	42	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	42	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	43	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	43	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	44	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	44	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	45	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	45	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	46	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	46	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	47	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	47	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	48	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	48	->'values'->0->>'value',	'blank'),
		coalesce(	"data"->'entries'->	49	->>'key',	'blank'), 	coalesce(	"data"->'entries'->	49	->'values'->0->>'value',	'blank')
    ) as first_tidy_json
    from scratch.deduplicated_discharges
);
