-- security.sql

-- 1. Create roles if they don't exist
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'Analyst') THEN
        CREATE ROLE "Analyst";
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'App_User') THEN
        CREATE ROLE "App_User";
    END IF;
END $$;

-- 2. Revoke default permissions from public
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;

-- 3. Configure Analyst to have select perms only
GRANT USAGE ON SCHEMA public TO "Analyst";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO "Analyst";

-- 4. Configure App_User to have select, insert, and update
GRANT USAGE ON SCHEMA public TO "App_User";
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO "App_User";

-- 5. Ensure future tables enforce these security policies
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO "Analyst";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON TABLES TO "App_User";