grant usage on schema derived to danielsilksmith if exists (select this from   pg_catalog.pg_rolesWHERE  rolname = 'danielsilksmith');
grant usage on schema scratch to danielsilksmith;
grant usage on schema derived to yalisassoon;
grant usage on schema scratch to yalisassoon;

grant usage on schema derived to deliwe;
grant usage on schema derived to farahshair; 
grant usage on schema derived to louisdutoit;
grant usage on schema derived to metabase_usr;

grant select on all tables in schema derived to danielsilksmith;
grant select on all tables in schema scratch to danielsilksmith; 
grant select on all tables in schema derived to yalisassoon;
grant select on all tables in schema scratch to yalisassoon;

grant select on all tables in schema derived to deliwe;
grant select on all tables in schema derived to farahshair;
grant select on all tables in schema derived to louisdutoit;
grant select on all tables in schema derived to metabase_usr;
grant select on all tables in schema derived to powerbi;
grant select on all tables in schema derived to powerbi_gateway;
