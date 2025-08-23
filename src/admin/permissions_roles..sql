/* permissions_setup.sql — Stap 4: Rechten & rollen (Azure SQL/SQL Server)
   Doel: ETL-rol kan loaders/dispatcher draaien (dynamic SQL ⇒ DML/SELECT nodig).
   Regels: 2-delige namen, geen @@ROWCOUNT. Volledig idempotent. */

SET NOCOUNT ON;

------------------------------------------------------------
-- 1) Rollen aanmaken (idempotent)
------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'role_etl_exec')
  CREATE ROLE [role_etl_exec];
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'role_readonly')
  CREATE ROLE [role_readonly];

------------------------------------------------------------
-- 2) Rechten voor ETL (dynamic SQL in loaders ⇒ directe DML/SELECT nodig)
--    - EXECUTE op dwh schema (procedures)
--    - DML op dwh schema (MERGE/INSERT/UPDATE/DELETE)
--    - SELECT op silver schema (brondata)
------------------------------------------------------------
GRANT EXECUTE ON SCHEMA::[dwh]   TO [role_etl_exec];
GRANT SELECT  ON SCHEMA::[silver] TO [role_etl_exec];
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::[dwh] TO [role_etl_exec];

------------------------------------------------------------
-- 3) Readonly-rol
--    - SELECT op dwh (rapportage/diagnose)
------------------------------------------------------------
GRANT SELECT ON SCHEMA::[dwh] TO [role_readonly];

------------------------------------------------------------
-- 4) (Optioneel) Specifieke grants indien je bronschema anders heet
-- GRANT SELECT ON SCHEMA::[bron] TO [role_etl_exec];

------------------------------------------------------------
-- 5) (Optioneel) Roltoewijzing voorbeelden (pas principal aan en deblokkeer)
-- ALTER ROLE [role_etl_exec]  ADD MEMBER [MyEtlAppUser];
-- ALTER ROLE [role_readonly]  ADD MEMBER [MyAnalystUser];
-- Om te verwijderen:
-- ALTER ROLE [role_etl_exec]  DROP MEMBER [MyEtlAppUser];
-- ALTER ROLE [role_readonly]  DROP MEMBER [MyAnalystUser];

------------------------------------------------------------
-- 6) Verificatie (overzicht)
------------------------------------------------------------
SELECT r.name AS role_name, m.name AS member_name
FROM sys.database_role_members drm
JOIN sys.database_principals r ON r.principal_id = drm.role_principal_id
JOIN sys.database_principals m ON m.principal_id = drm.member_principal_id
WHERE r.name IN (N'role_etl_exec', N'role_readonly')
ORDER BY r.name, m.name;

-- Controleer kernrechten snel
SELECT 'etl_exec_exec_dwh' AS check_name
WHERE HAS_PERMS_BY_NAME('dwh', 'SCHEMA', 'EXECUTE') = 1;
SELECT 'etl_exec_dml_dwh' AS check_name
WHERE HAS_PERMS_BY_NAME('dwh', 'SCHEMA', 'UPDATE') = 1
  AND HAS_PERMS_BY_NAME('dwh', 'SCHEMA', 'INSERT') = 1
  AND HAS_PERMS_BY_NAME('dwh', 'SCHEMA', 'DELETE') = 1
  AND HAS_PERMS_BY_NAME('dwh', 'SCHEMA', 'SELECT') = 1;
SELECT 'etl_exec_select_silver' AS check_name
WHERE HAS_PERMS_BY_NAME('silver', 'SCHEMA', 'SELECT') = 1;

-- Klaar. */
