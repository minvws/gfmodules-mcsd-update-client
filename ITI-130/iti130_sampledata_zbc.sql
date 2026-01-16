-- Voorbeelddata voor het minimalistische ZBC-schema met tblMedewerkerInzet

SET IDENTITY_INSERT dbo.tblKliniek ON;
INSERT INTO dbo.tblKliniek (
    KliniekId, Naam, AGBCode, KvKNummer, Actief, Telefoon, Email, Website,
    AdresRegel1, AdresRegel2, Postcode, Plaats, Land,
    FhirBaseUrl, FhirNotificationUrl, OAuthTokenUrl, LaatstGewijzigdOp
)
VALUES
(1, N'OrthoZBC B.V.', '08001234', '12345678', 1, N'+31-20-1234567', N'info@orthozbc.example', N'https://orthozbc.example',
 N'Amstelstraat 1', NULL, N'1017AB', N'Amsterdam', N'NL',
 N'https://api.orthozbc.example/fhir', N'https://notify.orthozbc.example/fhir', N'https://auth.orthozbc.example/oauth/token', SYSUTCDATETIME()),
(2, N'DermaZBC B.V.', '08005678', '87654321', 1, N'+31-30-7654321', N'info@dermazbc.example', N'https://dermazbc.example',
 N'Vechtdijk 10', NULL, N'3511AA', N'Utrecht', N'NL',
 N'https://api.dermazbc.example/fhir', N'https://notify.dermazbc.example/fhir', N'https://auth.dermazbc.example/oauth/token', SYSUTCDATETIME());
SET IDENTITY_INSERT dbo.tblKliniek OFF;

SET IDENTITY_INSERT dbo.tblLocatie ON;
INSERT INTO dbo.tblLocatie (
    LocatieId, KliniekId, Naam, LocatieType, Actief, Telefoon, Email,
    AdresRegel1, AdresRegel2, Postcode, Plaats, Land, Latitude, Longitude, LaatstGewijzigdOp
)
VALUES
(10, 1, N'OrthoZBC Amsterdam', N'vestiging', 1, N'+31-20-1234567', N'amsterdam@orthozbc.example',
 N'Amstelstraat 1', NULL, N'1017AB', N'Amsterdam', N'NL', 52.367600, 4.904100, SYSUTCDATETIME()),
(11, 1, N'OrthoZBC Amsterdam - Operatiecentrum', N'OK', 1, N'+31-20-1234567', N'ok@orthozbc.example',
 N'Amstelstraat 1', NULL, N'1017AB', N'Amsterdam', N'NL', 52.367600, 4.904100, SYSUTCDATETIME()),
(20, 2, N'DermaZBC Utrecht', N'vestiging', 1, N'+31-30-7654321', N'utrecht@dermazbc.example',
 N'Vechtdijk 10', NULL, N'3511AA', N'Utrecht', N'NL', 52.090700, 5.121400, SYSUTCDATETIME());
SET IDENTITY_INSERT dbo.tblLocatie OFF;

SET IDENTITY_INSERT dbo.tblAfdeling ON;
INSERT INTO dbo.tblAfdeling (
    AfdelingId, KliniekId, LocatieId, Naam, Actief,
    SpecialismeSystemUri, SpecialismeCode, SpecialismeDisplay, LaatstGewijzigdOp
)
VALUES
(100, 1, NULL, N'Orthopedie', 1, N'http://snomed.info/sct', N'394609007', N'Orthopedic medicine', SYSUTCDATETIME()),
(101, 1, 11, N'Pre-operatieve screening', 1, N'http://snomed.info/sct', N'700232004', N'Preoperative assessment', SYSUTCDATETIME()),
(200, 2, 20, N'Dermatologie', 1, N'http://snomed.info/sct', N'394582007', N'Dermatology', SYSUTCDATETIME());
SET IDENTITY_INSERT dbo.tblAfdeling OFF;

SET IDENTITY_INSERT dbo.tblMedewerker ON;
INSERT INTO dbo.tblMedewerker (
    MedewerkerId, KliniekId, LocatieId, AfdelingId, Actief,
    NaamWeergave, Achternaam, Voornaam, Tussenvoegsel,
    BIGNummer, AGBZorgverlenerCode,
    Email, Telefoon, Geslacht, Geboortedatum,
    RolCodeSystemUri, RolCode, RolDisplay, LaatstGewijzigdOp
)
VALUES
(1000, 1, 10, 100, 1, N'Dr. Anna Jansen', N'Jansen', N'Anna', NULL,
 '12345678901', '09001234', N'anna.jansen@orthozbc.example', N'+31-6-11111111', N'female', '1985-04-12',
 N'http://terminology.hl7.org/CodeSystem/practitioner-role', N'doctor', N'Doctor', SYSUTCDATETIME()),
(2000, 2, 20, 200, 1, N'Dr. Bram de Vries', N'de Vries', N'Bram', NULL,
 '10987654321', '09005678', N'bram.devries@dermazbc.example', N'+31-6-22222222', N'male', '1979-09-02',
 N'http://terminology.hl7.org/CodeSystem/practitioner-role', N'doctor', N'Doctor', SYSUTCDATETIME());
SET IDENTITY_INSERT dbo.tblMedewerker OFF;

-- Multi-inzet: Anna werkt zowel in Orthopedie (kliniekbreed) als in Pre-op screening (OK-locatie)
SET IDENTITY_INSERT dbo.tblMedewerkerInzet ON;
INSERT INTO dbo.tblMedewerkerInzet (
    InzetId, MedewerkerId, KliniekId, LocatieId, AfdelingId, Actief,
    StartDatum, EindDatum,
    RolCodeSystemUri, RolCode, RolDisplay,
    WerkPercentage, LaatstGewijzigdOp
)
VALUES
(1, 1000, 1, 10, 100, 1, '2023-01-01', NULL,
 N'http://terminology.hl7.org/CodeSystem/practitioner-role', N'doctor', N'Doctor', 80.00, SYSUTCDATETIME()),
(2, 1000, 1, 11, 101, 1, '2024-01-01', NULL,
 N'http://terminology.hl7.org/CodeSystem/practitioner-role', N'doctor', N'Doctor', 20.00, SYSUTCDATETIME()),
(3, 2000, 2, 20, 200, 1, '2022-06-01', NULL,
 N'http://terminology.hl7.org/CodeSystem/practitioner-role', N'doctor', N'Doctor', 100.00, SYSUTCDATETIME());
SET IDENTITY_INSERT dbo.tblMedewerkerInzet OFF;
