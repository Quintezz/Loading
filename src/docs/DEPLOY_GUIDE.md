# DEPLOY_GUIDE

Volgorde:
1. `deploy/01_schemas.sql`
2. `deploy/02_tables.sql`
3. `deploy/03_seeds.sql`
4. Deploy stored procedures uit `src/procs/`.
5. Configureer ADF met `pipelines/ADF/pl_dwh_refresh.json`.

Omgevingen:
- TST, ACC, PROD. Houd `Mode` en `IncrDate` per omgeving bij.
