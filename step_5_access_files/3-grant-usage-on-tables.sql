DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'danielsilksmith') THEN
        grant usage on schema derived to danielsilksmith AND
        grant usage on schema scratch to danielsilksmith AND 
        grant select on all tables in schema derived to danielsilksmith AND
        grant select on all tables in schema scratch to danielsilksmith
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'yalisassoon') THEN
        grant usage on schema derived to yalisassoon AND 
        grant usage on schema scratch to yalisassoon AND
        grant select on all tables in schema derived to yalisassoon AND
        grant select on all tables in schema scratch to yalisassoon
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'deliwe') THEN 
        grant usage on schema derived to deliwe AND 
        grant select on all tables in schema derived to deliwe AND 
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'farahshair') THEN 
        grant usage on schema derived to farahshair AND 
        grant select on all tables in schema derived to farahshair AND 
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'louisdutoit') THEN 
        grant usage on schema derived to louisdutoit AND 
        grant select on all tables in schema derived to louisdutoit AND 
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'metabase_usr') THEN 
        grant select on all tables in schema derived to metabase_usr AND 
        grant usage on schema derived to metabase_usr AND 
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'powerbi') THEN 
        grant select on all tables in schema derived to powerbi AND 
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'powerbi_gateway') THEN 
        grant select on all tables in schema derived to powerbi_gateway AND 
    END IF;
END
$$;







 







