# mCSD ITI-90 Address Book Proxy (FastAPI)

Deze app is een kleine **FastAPI**-proxy die eenvoudige queries vertaalt naar **FHIR (mCSD / ITI-90)**-searches op een upstream mCSD/FHIR-server en (waar nodig) resultaten “flattened” teruggeeft voor gebruik in een frontend.

**Legenda**
- **[PoC]** = PoC-/demo-specifiek (BgZ demo en PoC 8/9 endpoints).

---

## Operator / deploy

### Installatie (lokaal)

```bash
python -m venv .venv
. .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Configuratie

De app leest configuratie uit environment variabelen (en optioneel uit `.env` via `pydantic-settings`).
De tests stellen `MCSD_BASE=https://hapi.fhir.org/baseR4` automatisch in.

#### Upstream mCSD/FHIR base

`MCSD_BASE` is een **volledige base URL**.

Dat betekent het niet alleen de upstream server bepaalt, maar ook:

- het **protocol** (`http` of `https`)
- de **port** (optional)

Voorbeelden:

```bash
export MCSD_BASE=https://hapi.fhir.org/baseR4

export MCSD_BASE=https://mtls.fort365.net/address-book/admin-directory

# lokaal HTTP op port 8080
export MCSD_BASE=http://localhost:8080/mcsd

# HTTPS op port 8443
export MCSD_BASE=https://myserver:8443/mcsd

# HTTPS met default port 443
export MCSD_BASE=https://myserver/mcsd
```

Er zijn **geen** aparte variabelen voor protocol/poort; dit volgt volledig uit `MCSD_BASE`.

#### Timeouts en HTTP client

```bash
export MCSD_UPSTREAM_TIMEOUT=15
export MCSD_HTTPX_MAX_CONNECTIONS=50
export MCSD_HTTPX_MAX_KEEPALIVE_CONNECTIONS=20
```

#### Upstream authenticatie

Als je upstream een Bearer token verwacht:

```bash
export MCSD_BEARER_TOKEN="…"
```

De proxy voegt dan `Authorization: Bearer …` toe aan upstream requests.

#### TLS / certificaatverificatie

Bij gebruik van `https://`, kan je TLS verificatie instellen met:

```bash
export MCSD_VERIFY_TLS=true        # of false (niet aanbevolen)
export MCSD_CA_CERTS_FILE=/path/to/ca-bundle.pem   # optioneel
```

`MCSD_CA_CERTS_FILE` wordt alleen gebruikt als `MCSD_VERIFY_TLS=true`.

#### CORS en allowed hosts

Standaard staan CORS en host-checks “open” voor lokale ontwikkeling. Voor productie moet je dit dichtzetten.

```bash
export MCSD_ALLOW_ORIGINS='["https://jouw-frontend.example"]'
export MCSD_ALLOWED_HOSTS='["jouw-proxy.example"]'
```

> Let op: de parsing van list-waardes hangt af van je runtime/omgeving. In veel setups werkt JSON zoals hierboven; in andere setups wordt een komma-gescheiden string gebruikt. Test dit in je deployment-omgeving.

#### API key (optioneel)

Als je `MCSD_API_KEY` zet, zijn (bijna) alle endpoints beveiligd met een header:

- Header: `X-API-Key: <jouw key>`

```bash
export MCSD_API_KEY="supersecret"
```

Alleen `GET /health` blijft altijd zonder API key bereikbaar.

#### Productie guardrails

Als je `MCSD_IS_PRODUCTION=true` zet, faalt de app bij startup als één van deze onveilige defaults nog actief is:

- `MCSD_ALLOW_ORIGINS=["*"]`
- `MCSD_ALLOWED_HOSTS=["*"]`
- `MCSD_VERIFY_TLS=false`

```bash
export MCSD_IS_PRODUCTION=true
```

#### Query-limieten (bescherming)

Voor `GET /mcsd/search/{resource}` kun je limieten instellen:

```bash
export MCSD_MAX_QUERY_PARAMS=50
export MCSD_MAX_QUERY_VALUE_LENGTH=256
export MCSD_MAX_QUERY_PARAM_VALUES=20
```

#### [PoC] BgZ sender-identiteit (voor `POST /bgz/notify`)

De endpoint `POST /bgz/notify` verstuurt een **notified pull**-achtige Task namens een *vaste* afzender (PoC-sender).  
Om spoofing vanuit het frontend te voorkomen worden sender-waarden uit environment variabelen gelezen:

- `MCSD_SENDER_URA` — **verplicht**
- `MCSD_SENDER_NAME` — **verplicht**
- `MCSD_SENDER_BGZ_BASE` — optioneel (extra metadata/extensie in de Task)

Voorbeeld:

```bash
export MCSD_SENDER_URA=urn:oid:2.16.528.1.1007.3.3.1234567
export MCSD_SENDER_NAME="Mijn ZBC"
export MCSD_SENDER_BGZ_BASE=https://mijn-sender-fhir.example.org/fhir
```

Als `MCSD_SENDER_BGZ_BASE` ontbreekt, wordt die metadata niet meegestuurd.

#### [PoC] Audit logging en task preview (voor `POST /bgz/notify` en `POST /bgz/task-preview`)

Voor audit logging en (optioneel) het tonen van de uiteindelijke Task vóór verzending zijn er extra variabelen:

- `MCSD_AUDIT_HMAC_KEY` — optioneel. Als gezet, wordt gevoelige patiënt-identificatie (zoals BSN) **niet** als plain value gelogd maar als **HMAC-hash** (pseudonimisatie) in de audit logs.
- `MCSD_ALLOW_TASK_PREVIEW_IN_PRODUCTION` — optioneel (default `false`). Als `true`, is `POST /bgz/task-preview` ook beschikbaar als `MCSD_IS_PRODUCTION=true`.

Voorbeeld:

```bash
export MCSD_AUDIT_HMAC_KEY="een-lange-random-secret"
export MCSD_ALLOW_TASK_PREVIEW_IN_PRODUCTION=false
```

#### [PoC] Debug JSON dumps (voor `POST /bgz/load-data` en `POST /bgz/notify`)

Voor debug doeleinden kan de proxy de **outgoing JSON payloads** die deze endpoints naar een externe FHIR server sturen wegschrijven als bestanden op disk.

- `POST /bgz/load-data`: schrijft per verstuurde resource (PUT) één JSON bestand.
- `POST /bgz/notify`: schrijft één JSON bestand met de Task die naar `{receiver_notification_base}/Task` wordt gepost.

Dit staat standaard **uit** en is bedoeld voor lokale ontwikkeling; gebruik dit niet in productie omdat bestanden (ook met redactie) gevoelige data kunnen bevatten.

Environment variabelen:

- `MCSD_DEBUG_DUMP_JSON` — optioneel (default `false`). Zet op `true` om dumps te schrijven.
- `MCSD_DEBUG_DUMP_DIR` — optioneel (default `/tmp/mcsd-debug`). Directory waarin bestanden worden weggeschreven (moet writable zijn).
- `MCSD_DEBUG_DUMP_REDACT` — optioneel (default `true`). Redigeert bekende BSN-identifiers/velden in de JSON voordat deze naar disk gaat.

Voorbeeld:

```bash
export MCSD_DEBUG_DUMP_JSON=true
export MCSD_DEBUG_DUMP_DIR=/tmp/mcsd-debug
export MCSD_DEBUG_DUMP_REDACT=true
```

Bestandsnamen bevatten een timestamp en (als beschikbaar) de `X-Request-ID`, zodat je dumps makkelijk kunt koppelen aan applicatie-logs.


### Run

```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

### Draai tests (met de **publieke HAPI FHIR R4 server** op `https://hapi.fhir.org/baseR4`)

```bash
pytest -q
```

Notes:
- Tests will **skip gracefully** if the upstream is unreachable (e.g., network/firewall issues).  
- The proxy always sends `Accept: application/fhir+json` upstream, ensuring JSON responses.

FastAPI documentatie:
- Swagger UI: `/docs`
- OpenAPI spec: `/openapi.json`

### Observability / request-id

- Als de client een `X-Request-ID` header meestuurt, wordt die doorgegeven (en ook upstream gezet).
- Als die ontbreekt, genereert de proxy er één.
- De response bevat altijd `X-Request-ID`.

## Observability / audit logging

Naast gewone applicatie-logs (logger `mcsd.app`) schrijft de proxy ook **audit events** (logger `mcsd.audit`) als **JSON per regel**. Dit is bedoeld voor traceability van “business events” zoals het (proberen te) versturen van een Notified Pull notificatie.

Belangrijkste eigenschappen:

- Audit logs bevatten **geen** volledige Task payloads.
- Patiënt-identificatie wordt bij voorkeur **gepseudonimiseerd**: als `MCSD_AUDIT_HMAC_KEY` gezet is, wordt een HMAC-hash gelogd i.p.v. het BSN.
- De audit events bevatten o.a. `event_type`, `request_id`, `task_group_identifier`, `notification_endpoint_id` en `http_status` (bij resultaat).

Voorbeeld (conceptueel):

```json
{"event_type":"bgz.notify.attempt","request_id":"...","task_group_identifier":"urn:uuid:...","patient_ref":"hmac:...","resolved_receiver_base":"https://...","notification_endpoint_id":"Endpoint/..."}
{"event_type":"bgz.notify.result","request_id":"...","success":true,"http_status":201,"task_id":"...","task_group_identifier":"urn:uuid:..."}
```

### Audit logs scheiden van “tech logs”

Als je audit logs apart wilt wegschrijven (bijv. naar een apart bestand of een aparte log pipeline), configureer je logging zo dat logger `mcsd.audit` naar een eigen handler gaat.

Een eenvoudige manier is een eigen logging-config (JSON/YAML) voor uvicorn te gebruiken. Bijvoorbeeld: route `mcsd.audit` naar stdout of naar een file handler en zet `propagate=false` voor die logger.

---

## Frontend / API usage

### Authenticatie

Als `MCSD_API_KEY` is ingesteld, stuur dan bij elke call (behalve `GET /health`) een header mee:

```text
X-API-Key: <jouw key>
```

### Basis endpoints

#### `GET /health`

Liveness/readiness voor de proxy zelf.  
Dit endpoint controleert **niet** of de upstream mCSD server bereikbaar is.

Voorbeeld:

```bash
curl http://localhost:8000/health
```

#### `GET /mcsd/search/{resource}`

FHIR search “pass-through” met allow-list filtering. Alleen een vaste set resource types wordt geaccepteerd:

- `Practitioner`
- `PractitionerRole`
- `HealthcareService`
- `Location`
- `Organization`
- `Endpoint`
- `OrganizationAffiliation`

Niet-toegestane resources geven `400`.

Daarnaast wordt een allow-list per resource toegepast op query parameters (en `_count` wordt afgekapt op maximaal 200).

Voorbeeld:

```bash
curl "http://localhost:8000/mcsd/search/Organization?active=true&name:contains=ziekenhuis&_count=50"
```

### Addressbook convenience endpoints

#### `GET /addressbook/find-practitionerrole`

Convenience endpoint om eerst `Practitioner` te zoeken op naam en daarna bijbehorende `PractitionerRole` te halen.

Query parameters:
- `name` (verplicht)
- `organization` (optioneel)
- `specialty` (optioneel)

Voorbeeld:

```bash
curl "http://localhost:8000/addressbook/find-practitionerrole?name=Jansen"
```

#### `GET /addressbook/search`

Zoekt `Practitioner` + `PractitionerRole` en geeft “flattened” rows terug.  
Verrijkt daarnaast best-effort met:
- `HealthcareService` (op Organization of Location)
- `OrganizationAffiliation` relaties (met org-namen via `_include`)

Query parameters (selectie):
- `name`, `family`, `given`
- `organization` (bijv. `Organization/123`)
- `org_name` (client-side “contains” match)
- `specialty`
- `city`, `postal`
- `near` in vorm `lat|lng|distance|unit`
- `limit` (max 2000)
- `mode=fast|full` (default `fast`)

Aliases (ook toegestaan): `practitioner.name`, `practitioner.family`, `practitioner.given`, `practitioner.identifier`, `organization.name(:contains)`, `location.near`, `location.near-distance`, enz.

Voorbeeld:

```bash
curl "http://localhost:8000/addressbook/search?org_name=Oost&specialty=cardio&limit=50"
```

Response (globaal):
- `total`: aantal rows
- `rows`: lijst met velden zoals `practitioner_name`, `organization_name`, `email`, `phone`, `service_name`, `affiliation_*`, …

#### `GET /addressbook/organization`

Zoekt **organisaties** en retourneert functionele mailboxen:
- uit `Organization.telecom` (system=email)
- en (indien aanwezig) uit `Endpoint.address` met `mailto:...` via `_include=Organization:endpoint`

Query parameters:
- `name` (optioneel) of `name:contains`
- `active` (default `true`)
- `limit` (default `20`, max `100`)
- `contains` (boolean; alternatief voor `name:contains`)

Voorbeeld:

```bash
curl "http://localhost:8000/addressbook/organization?name:contains=ziekenhuis&limit=20"
```

#### `GET /addressbook/location`

Zoekt **locaties** en geeft de functionele mailbox van de zorgaanbieder (organisatie) terug:
- eerst `Location.telecom`
- daarna `Organization.telecom`
- daarna `Organization.endpoint` (mailto) indien beschikbaar

Query parameters:
- `name` (optioneel) of `name:contains`
- `limit` (default `20`, max `100`)
- `contains` (boolean; alternatief voor `name:contains`)

Voorbeeld:

```bash
curl "http://localhost:8000/addressbook/location?name:contains=polikliniek&limit=20"
```

---

## Capability mapping (PoC 9)

IG CodeSystem used in Endpoint.payloadType to declare data-exchange capabilities.
Reference: https://build.fhir.org/ig/nuts-foundation/nl-generic-functions-ig/CodeSystem-nl-gf-data-exchange-capabilities.html
IG_CAPABILITY_SYSTEM = "http://nuts-foundation.github.io/nl-generic-functions-ig/CodeSystem/nl-gf-data-exchange-capabilities"

Expected payloadType codes for PoC 9 capability mapping:

### REQUIRED for BgZ Notified Pull (TA Routering):
   - Code: "Twiin-TA-notification"
   - System: IG_CAPABILITY_SYSTEM (see above)
   - Purpose: Identifies the receiver's Task notification endpoint
   - Used in: Decision tree to find the endpoint for POST Task
   - Example mCSD entry:
     Endpoint.payloadType[].coding[] = {
       "system": "http://nuts-foundation.github.io/nl-generic-functions-ig/CodeSystem/nl-gf-data-exchange-capabilities",
       "code": "Twiin-TA-notification"
     }
### OPTIONAL for BgZ FHIR server discovery (informational):
   - Code: "http://nictiz.nl/fhir/CapabilityStatement/bgz2017-servercapabilities" (full URL as code)
   - System: "urn:ietf:rfc:3986" or may be absent
   - Purpose: Identifies the sender's BgZ FHIR server for receivers that cannot resolve via URA
   - Not required for sender-side notification flow

Note: The mCSD directory must populate Endpoint.payloadType with these codes
for the capability mapping to work correctly.

---

## PoC endpoints

### [PoC] BgZ endpoints

#### `POST /bgz/load-data`

Laadt BgZ sample data (Patient/Condition/Allergy/Medication/…) naar een doel-FHIR server (bijv. HAPI) met `PUT` per resource.

Query parameters:
- `hapi_base` (verplicht): base URL van de doelsserver
- `sender_ura` (verplicht): URA/OID die in het sample bundle wordt gezet

Voorbeeld:

```bash
curl -X POST "http://localhost:8000/bgz/load-data?hapi_base=http://localhost:8080/fhir&sender_ura=12345678"
```

> PoC-only: niet bedoeld voor productie, omdat dit demo-data laadt.

#### `POST /bgz/notify`

Stuurt een notificatie **Task** (notified pull pattern) naar de receiver (`{receiver_notification_base}/Task`).  
De sender-identiteit komt uit environment variabelen (`MCSD_SENDER_URA`, `MCSD_SENDER_NAME`, optioneel `MCSD_SENDER_BGZ_BASE`).

**SSRF-mitigatie:** de client geeft geen vrije `receiver_base` mee. In plaats daarvan resolveert de backend `receiver_notification_base` opnieuw via het mCSD-adresboek (`MCSD_BASE`) op basis van `receiver_target_ref` (en optioneel `receiver_org_ref`) door de PoC 9 capability mapping te gebruiken. Daarbij wordt een `Endpoint` gezocht met payloadType `Twiin-TA-notification`, waarna `Endpoint.address` wordt genormaliseerd naar een base (o.a. trailing `/` en eventuele `/Task` eraf) en vervolgens wordt gevalideerd als een http(s)-URL.

Audit logging: bij elke poging en uitkomst van `POST /bgz/notify` wordt een audit event gelogd via logger `mcsd.audit` (zie Observability / audit logging).

Request body (JSON):
- `receiver_target_ref` (verplicht): `Organization/<id>`, `HealthcareService/<id>` of `Location/<id>`
- `receiver_org_ref` (optioneel): `Organization/<id>` (kan helpen bij capability mapping als het target zelf geen endpoints heeft)
- `receiver_ura` (verplicht)
- `receiver_name` (verplicht): display name van receiver (bijv. “Ziekenhuis Oost – Cardiologie”)
- `receiver_org_name` (optioneel): display name van de receiver-organisatie (voor `Task.owner.display`)
- `patient_bsn` (verplicht)
- `patient_name` (optioneel)
- `description` (optioneel)

Voorbeeld:

```bash
curl -X POST "http://localhost:8000/bgz/notify" \
  -H "Content-Type: application/json" \
  -d '{
    "receiver_target_ref": "HealthcareService/123",
    "receiver_org_ref": "Organization/456",
    "receiver_ura": "87654321",
    "receiver_name": "Ziekenhuis Oost - Cardiologie",
    "receiver_org_name": "Ziekenhuis Oost",
    "patient_bsn": "172642863",
    "patient_name": "J.P. van der Berg",
    "description": "BgZ notified pull demo"
  }'
```

**Foutafhandeling (hard fail)**

Als er via PoC9 capability mapping géén `Endpoint` met payloadType `Twiin-TA-notification` gevonden kan worden voor het gekozen `receiver_target_ref` (en optioneel `receiver_org_ref`), dan kan de backend geen veilige `receiver_notification_base` bepalen. In dat geval wordt er **geen** notificatie verstuurd en retourneert de API een **HTTP 400**:

```json
{
  "detail": {
    "reason": "no_notification_endpoint",
    "message": "Geen Twiin TA notification endpoint gevonden voor het gekozen target/organisatie."
  }
}
```

Dit is een “hard fail”: de caller moet eerst zorgen dat in het mCSD-adresboek een geschikt (actief) Twiin-TA-notification Endpoint aanwezig is en daarna opnieuw notificeren.

### [PoC] PoC 8/9 (MSZ) endpoints

Deze endpoints ondersteunen PoC 8/9 UI-flows (MSZ organisaties, organisatieonderdelen en technische endpoints).  
Ze hebben cursor-based paginering voor “meer laden” in de UI.

#### `GET /poc9/msz/organizations`

Zoekt MSZ-zorgorganisaties (Organization) en levert per org ook technische endpoint-info (op basis van `_include=Organization:endpoint`).

Query parameters:
- `name` (optioneel), `contains` (boolean)
- `identifier` (optioneel)
- `type` (optioneel; query-param alias)
- `limit` (default 20)
- `cursor` (optioneel; voor volgende pagina)

Response bevat o.a. `next` (opaque cursor) en `total` (upstream total, indien aanwezig).

#### `GET /poc9/msz/orgunits`

Zoekt organisatieonderdelen binnen een org:
- `kind=location|service|suborg|all`
- `organization` is verplicht in de eerste call (zonder `cursor`)
- `cursor` voor volgende pagina’s

#### `GET /poc9/msz/endpoints`

Haalt “technische endpoints” op voor een geselecteerd target (`Location/…`, `HealthcareService/…`, `Organization/…`).

Query parameters:
- `target` (verplicht zonder cursor): `ResourceType/id`
- optioneel filters: `endpoint_kind` (heuristisch), `connection_type`, `payload_type`, `payload_mime_type`
- `limit`, `cursor`

#### `GET /poc9/msz/capability-mapping`

Resolve’t endpoints voor capabilities via decision tree A–D (PoC 9).  
Required: Twiin TA notificatie capability. Optioneel: BgZ FHIR server capability en Nuts OAuth (`include_oauth=true`).

Query parameters:
- `target` (verplicht): `ResourceType/id`
- `organization` (optioneel): `Organization/id` (anders probeert de service dit af te leiden)
- `include_oauth` (optioneel)
- `limit` (max endpoints per scope)

---

## Verschillen met upstream / foutafhandeling

- Upstream HTTP fouten (4xx/5xx) worden doorgaans doorgegeven met dezelfde statuscode en JSON body (als die JSON is).
- Connectieproblemen naar upstream worden als `502` teruggegeven met een `detail` object met een `reason` zoals `timeout`, `dns`, `tls` of `network`.
