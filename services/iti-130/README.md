# iti130_publisher.py – Gebruikshandleiding

Deze publisher leest ZBC/EPD-tabellen (o.a. kliniek, locatie, afdeling, endpoint en optioneel medewerker/inzet) en publiceert daaruit afgeleide mCSD/FHIR Directory resources naar een FHIR-server via een **transaction Bundle** (IHE **ITI‑130 Care Services Feed**).

De output bestaat (minimaal) uit:

- **Organization** (kliniek + afdeling als child-Organization)
- **Location** (locatie)
- **HealthcareService** (1:1 afgeleid van afdeling)
- **Endpoint** (afgeleid van `tblEndpoint`)
- Optioneel: **Practitioner** + **PractitionerRole** (medewerker + inzet)

> De logische FHIR ids zijn stabiel en worden afgeleid van de table keys (geen extra FhirId kolommen nodig), bijv. `org-kliniek-<id>`, `loc-<id>`, `ep-<id>`, `prac-<id>`, etc.

---

## Vereisten

- Python 3
- Voor **SQLite** kan het script draaien zonder SQLAlchemy (valt terug op de ingebouwde `sqlite3` module).
- Voor **MS SQL Server** gebruikt het script bij voorkeur **SQLAlchemy** met een MSSQL driver (bijv. `mssql+pytds://...` of via `pyodbc`).

---

## Snel starten

### 1) SQLite (demo / lokaal testen)

```bash
python iti130_publisher.py \
  --sql-conn sqlite:///demo.db \
  --fhir-base https://fhir.example.org/fhir \
  --dry-run \
  --out bundle.json
```

- Bij SQLite initialiseert het script automatisch een klein schema (en seed data als de tabellen leeg zijn).
- Gebruik `--sqlite-reset-seed` (of env `SQLITE_RESET_SEED=1`) om bestaande demo data te wissen en de seed opnieuw aan te maken.

#### Seed data (alleen SQLite demo)

Wanneer je `--sql-conn sqlite:///...` gebruikt, initialiseert het script (als de tabellen leeg zijn) **automatisch demo seed data**. Dit gebeurt alleen als `tblKliniek` nog geen records bevat.

Wil je de demo-seed forceren (bijv. nadat je zelf testdata hebt toegevoegd), gebruik dan:

```bash
python iti130_publisher.py \
  --sql-conn sqlite:///demo.db \
  --sqlite-reset-seed \
  --fhir-base https://fhir.example.org/fhir \
  --dry-run \
  --out bundle.json
```

Of via environment variabele (handig in CI):

```bash
export SQLITE_RESET_SEED=1
```

> **Let op:** dit is **destructief** voor het opgegeven SQLite bestand: het verwijdert alle rijen uit de demo-tabellen (`tblKliniek`, `tblLocatie`, `tblAfdeling`, `tblEndpoint`, en bij practitioners ook `tblMedewerker*`/`tblRoldefinitie`).

De seed is bedoeld voor **lokaal testen** en bestaat uit één demo kliniek met één locatie, twaalf afdelingen en één endpoint. Daarnaast wordt één medewerker + inzet + roldefinitie gevuld (alleen relevant als je `--include-practitioners` gebruikt).

**Kliniek (tblKliniek → Organization)**

| Veld | Waarde |
|---|---|
| kliniekkey | `1` |
| naam | `ZBC Demo Kliniek` |
| URA | `00700700` |
| AGB | `00000000` |
| KvK | `12345678` |
| Actief | `1` |
| Adres | Demo Straat 1, 1011AA Amsterdam (NL) |
| Type | `Healthcare Provider` |
| Telefoon / email / website | `+31-20-0000000`, `info@demo.invalid`, `https://demo.invalid` |

→ FHIR id: `Organization/org-kliniek-1`

**Locatie (tblLocatie → Location)**

| Veld | Waarde |
|---|---|
| locatiekey | `10` |
| naam | `Demo Locatie` |
| Type | `Hospital` |
| Actief | `1` |
| Adres | Locatie Straat 10, 1011AA Amsterdam (NL) |
| GPS | `52.3702`, `4.8952` |
| AGB | `00000000` |

→ FHIR id: `Location/loc-10`  
→ Relatie met kliniek via `tblKliniekLocatie (1 ↔ 10)`

**Afdelingen (tblAfdeling → Organization + HealthcareService)**

| afdelingkey | naam | Kliniek | Locatie | Specialisme (SNOMED) | Actief |
|---|---|---|---|---|---|
| `100` | `Beweegpoli` | `1` | `10` | `1251536003` (*Sport medicine*) | `1` |
| `101` | `Dermatologie (huidziekten)` | `1` | `10` | `394582007` (*Dermatology*) | `1` |
| `102` | `Interne geneeskunde` | `1` | `10` | `419192003` (*Internal medicine*) | `1` |
| `103` | `Leefstijl coaching` | `1` | `10` | `722164000` (*Dietetics and nutrition*) | `1` |
| `104` | `Orthopedie` | `1` | `10` | `394801008` (*Trauma & orthopaedics*) | `1` |
| `105` | `Penispoli` | `1` | `10` | `394612005` (*Urology*) | `1` |
| `106` | `Plastische chirurgie` | `1` | `10` | `394611003` (*Plastic surgery*) | `1` |
| `107` | `Proctologie (anus problemen)` | `1` | `10` | `408464004` (*Colorectal surgery*) | `1` |
| `108` | `Reumatologie (ontstekingen gewrichten)` | `1` | `10` | `394810000` (*Rheumatology*) | `1` |
| `109` | `Spatader- & wondzorg (vaatchirurgie en dermatologie)` | `1` | `10` | `408463005` (*Vascular surgery*) | `1` |
| `110` | `Vasectomie/sterilisatie` | `1` | `10` | `394612005` (*Urology*) | `1` |
| `111` | `Vulvapoli (derma en gynaecologie)` | `1` | `10` | `394586005` (*Gynaecology*) | `1` |

→ FHIR ids (per afdelingkey): `Organization/org-afdeling-{key}` en `HealthcareService/svc-afdeling-{key}`
**Endpoint (tblEndpoint → Endpoint)**

| Veld | Waarde |
|---|---|
| endpointkey | `900` |
| Kliniek | `1` |
| Status | `active` |
| Address | `https://fhir.demo.invalid` |
| ConnectionType | HL7 FHIR REST |
| payloadTypeSystemUri | `http://nuts-foundation.github.io/nl-generic-functions-ig/CodeSystem/nl-gf-data-exchange-capabilities` |
| payloadTypeCode | `http://nictiz.nl/fhir/CapabilityStatement/bgz2017-servercapabilities` |
| payloadTypeDisplay | `BGZ Server` |
| payloadMimeType | `application/fhir+json` |

→ FHIR id: `Endpoint/ep-900`

> Let op: in de seed zijn `payloadType*` velden gevuld voor **BGZ Server**. Als je in je eigen brondata `payloadType*` leeg laat, gebruikt het script de default payloadType(s) voor BGZ (tenzij je dit overschrijft met `--default-endpoint-payload`).

**Practitioner seed (alleen bij `--include-practitioners`)**

- `tblRoldefinitie`: `doctor` / “Doctor”
- `tblMedewerker`: `medewerkerkey=1000` (Dr. John Smith), BIG `12345678901`, AGB zorgverlener `99999999`, default locatie `10`
- `tblMedewerkerinzet`: `inzetkey=5000` koppelt medewerker `1000` aan afdeling `100` (Beweegpoli), start `2020-01-01`, rol `doctor`

**Relaties (kort)**

```
Organization/org-kliniek-1
 ├─ Endpoint/ep-900
 ├─ Location/loc-10
 │   └─ Organization/org-afdeling-100
 │       └─ HealthcareService/svc-afdeling-100
 │           └─ (optioneel) Practitioner/prac-1000 → PractitionerRole/pracrole-5000
```


### 2) MS SQL Server

```bash
python iti130_publisher.py \
  --sql-conn "mssql+pytds://user:pass@host:1433/DBNAME" \
  --fhir-base https://fhir.example.org/fhir \
  --token "YOUR_BEARER_TOKEN"
```

### 3) OAuth2 client_credentials i.p.v. een vast bearer token

```bash
python iti130_publisher.py \
  --sql-conn "mssql+pytds://user:pass@host:1433/DBNAME" \
  --fhir-base https://fhir.example.org/fhir \
  --oauth-token-url https://auth.example.org/oauth/token \
  --oauth-client-id "client-id" \
  --oauth-client-secret "client-secret" \
  --oauth-scope "scope-a scope-b"
```


---

## Wat moet je instellen (1A t/m 1G)

Hieronder een praktische checklist van de belangrijkste instellingen voor het publiceren van organisatiegegevens naar een Administration Directory (FHIR server) via ITI‑130.

### A) Waar publiceer je naartoe? `--fhir-base`

- Stel `--fhir-base` (of env `FHIR_BASE`) in op de base URL van de FHIR server die jullie **Administration Directory** vormt en transaction Bundles accepteert.
- Gebruik bij voorkeur `https://...` (in `--production` mode is `http://` niet toegestaan).

### B) Waar komt de brondata vandaan? `--sql-conn`

- Stel `--sql-conn` (of env `SQL_CONN`) in op de database waarin de bron-tabellen/views staan (`tblKliniek`, `tblLocatie`, `tblAfdeling`, `tblEndpoint`, ...).
- Voor DiSy kun je desgewenst views maken met deze namen/kolommen zodat het script zonder codewijziging kan draaien.

### C) Authenticatie / TLS naar de FHIR server

Kies één van de volgende opties (afhankelijk van de directory):

- Bearer token: `--token` (of env `FHIR_TOKEN`)
- OAuth2 client_credentials: `--oauth-token-url`, `--oauth-client-id`, `--oauth-client-secret` (en optioneel `--oauth-scope`)
- Mutual TLS: `--mtls-cert` en optioneel `--mtls-key`
- Alleen voor test: `--no-verify-tls` (niet aanbevolen; wordt error in `--production`)

### D) NL GF profielen en identifiers

- Voor NL Generic Functions (GF) Adressering gebruik je doorgaans:
  - `--include-meta-profile`
  - `--profile-set nl` (default is `nl`)
- Zet `--assigned-id-system-base` op een eigen, stabiele URI namespace (niet de default `https://sys.local/identifiers`).
- Zorg dat de URA bepaald kan worden via `tblKliniek.uranummer` of gebruik `--default-ura` als fallback.

### E) Endpoints (vindbaarheid & routering)

- Vul `tblEndpoint` met de technische endpoints die andere partijen moeten kunnen vinden:
  - `adres` (wordt `Endpoint.address`) is verplicht en moet een URL zijn.
  - `status`/`actief`/`ingangsdatum`/`einddatum` bepalen of het endpoint effectief “active” is.
  - `payloadType*` bepaalt waarvoor het endpoint bedoeld is (bijv. BgZ).
- Als `payloadType*` leeg is, gebruikt het script default(s) voor BGZ (tenzij je dit overschrijft met `--default-endpoint-payload`).

#### Voorbeeld: BGZ Server capabilities invullen in `tblEndpoint` (MS SQL)

Voor BGZ (MSZ/BgZ use case) hoort `Endpoint.payloadType` een coding te bevatten uit de NL GF *Data exchange capabilities* codeset.
Vul daarvoor in de brondata (dbo.`tblEndpoint`) de kolommen `payloadTypeSystemUri`, `payloadTypeCode` en `payloadTypeDisplay`.

Voorbeeld (update één bestaand endpoint):

```sql
UPDATE dbo.tblEndpoint
SET payloadTypeSystemUri = 'http://nuts-foundation.github.io/nl-generic-functions-ig/CodeSystem/nl-gf-data-exchange-capabilities',
    payloadTypeCode = 'http://nictiz.nl/fhir/CapabilityStatement/bgz2017-servercapabilities',
    payloadTypeDisplay = 'BGZ Server'
WHERE endpointkey = 900;
```

Voorbeeld (bij het aanmaken van een nieuw endpoint record):

```sql
INSERT INTO dbo.tblEndpoint (
    endpointkey, kliniekkey, locatiekey, afdelingkey,
    status, connectionTypeSystemUri, connectionTypeCode, connectionTypeDisplay,
    payloadTypeSystemUri, payloadTypeCode, payloadTypeDisplay, payloadMimeType,
    adres, naam, telefoon, email,
    ingangsdatum, einddatum, actief, LaatstGewijzigdOp
) VALUES (
    901, 1, NULL, NULL,
    'active', 'http://terminology.hl7.org/CodeSystem/endpoint-connection-type', 'hl7-fhir-rest', 'HL7 FHIR REST',
    'http://nuts-foundation.github.io/nl-generic-functions-ig/CodeSystem/nl-gf-data-exchange-capabilities', 'http://nictiz.nl/fhir/CapabilityStatement/bgz2017-servercapabilities', 'BGZ Server', 'application/fhir+json',
    'https://fhir.example.org/fhir', 'FHIR API', '+31-20-0000000', 'fhir@example.org',
    '2020-01-01', NULL, 1, SYSUTCDATETIME()
);
```

> Tip: als je `payloadType*` leeg laat in je brondata, kun je ook via de CLI defaults zetten met `--default-endpoint-payload`.
> Voor productie (en troubleshooting) is expliciet vullen in `tblEndpoint` meestal het duidelijkst.

### F) BGZ sanity policy (kan een run laten falen)

- Met `--bgz-policy` bepaal je hoe strikt het script controleert dat er BGZ-capable endpoints aanwezig zijn.
- Gebruik in de MSZ/BgZ use case meestal `per-clinic` (default) of, als jullie endpoints op afdelingsniveau zitten, `per-afdeling` / `per-afdeling-or-clinic`.

### G) Testen zonder te posten

- `--dry-run` print de transaction Bundle en doet geen POST.
- `--out <bestand.json>` schrijft de Bundle naar een bestand (alleen in combinatie met `--dry-run`).


## URA-nummer (UZI-register abonneenummer)

Voor NL Generic Functions (GF) Adressering is de **URA** de leidende identificatie van de zorgorganisatie. Dit script gebruikt de URA daarom consequent in de Directory-resources.

### Waar wordt de URA gebruikt?

- In **Organization.identifier** met `system=http://fhir.nl/fhir/NamingSystem/ura`.
- In **Location.identifier** en **HealthcareService.identifier** (naast de NL GF `identifier:AssignedId`).
- In de `assigner` van de **NL GF AssignedId** identifiers (`identifier.assigner.identifier.system/value`), zodat zichtbaar is **welke organisatie** de author-assigned identifiers uitgeeft.

### Waar komt de URA vandaan?

- **Productie:** gebruik de URA van je zorgorganisatie zoals geregistreerd bij het **UZI-register** (deze staat o.a. op het UZI-servercertificaat / stamkaart).
- **PoC/test:** vaak wordt een **fake URA** afgesproken. Gebruik dan `--publisher-ura` (of env `PUBLISHER_URA`) om deze te forceren zonder brondata te wijzigen.

### Prioriteit / configuratie

De URA wordt bepaald in deze volgorde:

1. `--publisher-ura` (of `PUBLISHER_URA`) – **hard override** (aanrader voor PoC/test; maakt switch PoC↔prod mogelijk zonder DB-wijziging).
2. `URANummer` in de brondata (`tblKliniek.uranummer`).
3. `--default-ura` (of `DEFAULT_URA`) – fallback als brondata geen URA bevat.

### Normalisatie

- Een prefix `URA:` wordt gestript (bijv. `URA:12345` → `12345`).
- Als de URA numeriek is en **korter dan 8 cijfers**, dan padt het script links met nullen tot 8 cijfers (bijv. `12345` → `00012345`).

### Voor PoC 9 staat de URA in tblKliniek.uranummer en is poc9-sys-001.

Alle gepubliceerde Organization/Location/HealthcareService resources gebruiken deze URA.


---

## Delta publiceren met `--since`

Met `--since` publiceer je **best‑effort alleen wijzigingen sinds een timestamp** (UTC, ISO‑format), in plaats van telkens alles opnieuw.

Voorbeeld:

```bash
python iti130_publisher.py \
  --sql-conn "mssql+pytds://user:pass@host:1433/DBNAME" \
  --fhir-base https://fhir.example.org/fhir \
  --since 2025-12-30T12:00:00Z
```

### Kanttekeningen bij `--since`

- `--since` is **geen ITI‑130 protocol parameter**; het is puur een **publish-selectie** in dit script.
- De selectie is grotendeels gebaseerd op `LaatstGewijzigdOp >= since`.
- Voor **Endpoint** (en bij practitioners ook voor inzet/rollen) wordt daarnaast gekeken naar **StartDatum/EindDatum** die *tussen since en vandaag* vallen. Dat vangt “status flips” af die kunnen gebeuren zonder dat `LaatstGewijzigdOp` wijzigt.
- Bij delta-publicatie kan het voorkomen dat een resource in de bundle verwijst naar een resource die **niet** in dezelfde run wordt meegestuurd (omdat die al eerder is gepubliceerd).  
  In dat geval geeft de sanity check **warnings i.p.v. errors**.
- Delta-publicatie blijft “best‑effort”: als bron-timestamps niet betrouwbaar zijn, of als er ingrijpende mapping-wijzigingen zijn, kan een periodieke **full publish** nodig zijn.

---

## CLI gebruik

Algemene vorm:

```bash
python iti130_publisher.py [opties]
```

Het script leest ook defaults uit environment variabelen en/of een `.env` bestand (CLI overrides environment).

---

## CLI opties

### Database & FHIR server

- `--sql-conn`  
  Database verbinding. SQLAlchemy URL (aanrader) zoals `sqlite:///pad.db` of `mssql+pytds://user:pass@host:1433/db`.  
  Een “legacy” ODBC string zonder `://` kan ook, maar vereist `pyodbc`.

- `--sqlite-reset-seed`  
  **Alleen SQLite demo:** wis bestaande demo-data uit de SQLite database en maak de ingebouwde seed data opnieuw aan.  
  (Destructief; bedoeld voor lokaal testen / CI. Alternatief: env `SQLITE_RESET_SEED=1`.)

- `--fhir-reset-seed`  
  Wis alle resources op de FHIR server voordat er gepubliceerd wordt.  
  (Destructief; bedoeld voor lokaal testen / CI. Alternatief: env `FHIR_RESET_SEED=1`.)

- `--fhir-base`  
  Base URL van de FHIR server waar je transaction Bundles naar POST.

### Authenticatie

- `--token`  
  Bearer token (heeft voorrang op OAuth instellingen).

- `--oauth-token-url`  
  OAuth2 token endpoint (client_credentials).

- `--oauth-client-id`  
  OAuth2 client id.

- `--oauth-client-secret`  
  OAuth2 client secret.

- `--oauth-scope`  
  OAuth scope(s) (optioneel).

### TLS / mTLS

- `--no-verify-tls`  
  Zet TLS verificatie uit (niet aanbevolen; in `--production` mode wordt dit een error).

- `--mtls-cert`  
  Client certificaat (PEM). Als `--mtls-key` niet gezet is moet de private key hierin zitten.

- `--mtls-key`  
  Private key (PEM) voor mutual TLS.

### Publicatiegedrag

- `--bundle-size`  
  Max aantal entries per transaction bundle. Grotere aantallen worden in meerdere transacties opgesplitst.

- `--since`  
  Publiceer alleen wijzigingen sinds deze UTC timestamp (ISO).

- `--dry-run`  
  Post niet naar de server; print de Bundle JSON naar stdout.

- `--out`  
  Schrijf Bundle JSON naar bestand (alleen met `--dry-run`).

### Profielen / NL GF specifieke opties

- `--include-meta-profile`  
  Voeg `meta.profile` toe aan resources.

- `--profile-set {nl|ihe|none}`  
  Welke profile set je declareert als `--include-meta-profile` aan staat.

- `--assigned-id-system-base`  
  Base URI voor author-assigned identifier systems (NL GF AssignedId slices).  
  Voorbeeld: `https://example.org/identifiers`

- `--default-ura`  
  Fallback URA wanneer brondata geen URA bevat (of niet naar een kliniek URA te mappen is). Wordt gebruikt als er geen URA uit brondata of `--publisher-ura` te bepalen is.

- `--publisher-ura` / `--ura`  
  Hard override voor de URA van de publicerende zorgorganisatie. Handig voor PoC/test (fake URA) en om zonder DB-wijziging te switchen naar productie. Overschrijft `URANummer` uit de brondata.

- `--default-endpoint-payload` (repeatable)  
  Default `Endpoint.payloadType` wanneer de bronregel geen payloadType heeft.  
  Formaat: `system|code|display`

- `--bgz-policy {off|any|per-clinic|per-afdeling|per-afdeling-or-clinic}`  
  Extra sanity policy voor BGZ endpoints.

### Practitioners

- `--include-practitioners`  
  Publiceer ook Practitioner en PractitionerRole uit `tblMedewerker` en `tblMedewerkerinzet`.
  Dit is buiten scope van de PoC en daarvoor dus niet vereist.

### Delete policy

- `--delete-inactive`  
  Publiceer DELETE voor “inactieve” records i.p.v. PUT met `active=false`/`status=inactive`.

- `--allow-delete-endpoint`  
  Sta DELETE voor Endpoint toe als `--delete-inactive` aan staat (niet aanbevolen; NL GF adviseert meestal `Endpoint.status=off`).

### Lenient mode

- `--lenient`  
  Schakel strict mapping uit: ontbrekende inputs geven warnings en veilige fallbacks/placeholder values.

### HTTP tuning

- `--timeout`  
  HTTP read timeout in seconden.

- `--connect-timeout`  
  HTTP connect timeout in seconden.

- `--http-retries`  
  Aantal retries bij tijdelijke fouten (0 = uit).

- `--http-backoff`  
  Backoff factor voor retries.

- `--http-pool-connections`  
  Connection pool: aantal pools.

- `--http-pool-maxsize`  
  Connection pool: max connections per pool.

### Observability / safety

- `--log-level`  
  Log level (DEBUG/INFO/WARNING/ERROR).

- `--production`  
  Behandel “risicovolle” instellingen als errors (bijv. http:// fhir-base, `--no-verify-tls`, SQL encrypt hints).

---

## Environment variabelen

De volgende environment variabelen kunnen als defaults gebruikt worden (ook via `.env`). CLI parameters hebben voorrang.

### Basis

- `SQL_CONN` → `--sql-conn`
- `FHIR_BASE` → `--fhir-base`

### SQLite demo

- `SQLITE_RESET_SEED` → `--sqlite-reset-seed` (1/true = wis demo-data en maak de seed opnieuw)

### FHIR reset

- `FHIR_RESET_SEED` → `--fhir-reset-seed` (1/true = wis alle resources op de FHIR server)

### Auth

- `FHIR_TOKEN` → `--token`
- `OAUTH_TOKEN_URL` → `--oauth-token-url`
- `OAUTH_CLIENT_ID` → `--oauth-client-id`
- `OAUTH_CLIENT_SECRET` → `--oauth-client-secret`
- `OAUTH_SCOPE` → `--oauth-scope`

### Publish gedrag

- `BUNDLE_SIZE` → `--bundle-size`
- `SINCE_UTC` → `--since`

### Profiel / NL GF

- `PROFILE_SET` → `--profile-set`
- `ASSIGNED_ID_SYSTEM_BASE` → `--assigned-id-system-base`
- `DEFAULT_URA` → `--default-ura`
- `PUBLISHER_URA` → `--publisher-ura` (hard override; PoC/test)
- `BGZ_POLICY` → `--bgz-policy`

### mTLS

- `MTLS_CERT` → `--mtls-cert`
- `MTLS_KEY` → `--mtls-key`

### HTTP tuning

- `HTTP_TIMEOUT` → `--timeout`
- `HTTP_CONNECT_TIMEOUT` → `--connect-timeout`
- `HTTP_RETRIES` → `--http-retries`
- `HTTP_BACKOFF` → `--http-backoff`
- `HTTP_POOL_CONNECTIONS` → `--http-pool-connections`
- `HTTP_POOL_MAXSIZE` → `--http-pool-maxsize`

### Logging

- `ITI130_LOG_LEVEL` → `--log-level`

---

## Known issues / patch notes

### Geen  

---

## Tips voor beheer

- Voor een **initiële load**: draai zonder `--since` en overweeg een grotere `--bundle-size` zodat referenties vaker in dezelfde transaction zitten.
- Voor **periodieke updates**: draai met `--since` en beheer de “laatste succesvolle timestamp” extern (scheduler/CI).
- Als je `--delete-inactive` gebruikt: wees voorzichtig met Endpoint DELETE; NL GF adviseert meestal “status=off”.

