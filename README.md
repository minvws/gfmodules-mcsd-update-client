# mCSD Consumer

This app is the mCSD (Mobile Care Service Discovery) Consumer and is part of
the 'Generieke Functies, lokalisatie en addressering' project of the Ministry of Health, Welfare and Sport of the Dutch government. The purpose of this application is
to perform updates on [mCSD Supported Resources](https://profiles.ihe.net/ITI/mCSD/index.html).
The update mechanism is based on [ITI-91: Request Care Services Update](https://profiles.ihe.net/ITI/mCSD/ITI-91.html).
The update uses http as a basis for CRUD operations, and operates regardless of
the FHIR store type.

## Disclaimer

This project and all associated code serve solely as documentation
and demonstration purposes to illustrate potential system
communication patterns and architectures.

This codebase:

- Is NOT intended for production use
- Does NOT represent a final specification
- Should NOT be considered feature-complete or secure
- May contain errors, omissions, or oversimplified implementations
- Has NOT been tested or hardened for real-world scenarios

The code examples are only meant to help understand concepts and demonstrate possibilities.

By using or referencing this code, you acknowledge that you do so at your own
risk and that the authors assume no liability for any consequences of its use.

## Setup

In order to test the update mechanism, you need at least two other instances of
a FHIR store. One instance as a [Consumer](https://profiles.ihe.net/ITI/mCSD/volume-1.html#146113-care-services-update-consumer)
and at least one instance as a
[Provider](https://profiles.ihe.net/ITI/mCSD/volume-1.html#146113-care-services-update-consumer).
You can either use a [HAPI JPA server](https://hapifhir.io/hapi-fhir/),  
[addressing-register](https://github.com/minvws/gfmodules-addressing-register) or
any other FHIR store of your choosing as long as they support mCSD specs.

Follow the instructions to get the app running:

- if you want to run the application by itself, open this folder in a terminal and execute these commands:

```bash
cd example-setup-with-hapi
docker compose up
```

This will configure the whole system for you and you should be able to use the
API right away at <https://localhost:8509/docs>.

## Seeding supplier

In the case you want to seed a mock supplier with fake data, first set up the correct
URL of the mock supplier in the app.conf under `mock_seeder`.
Finally, in a terminal in the same `example-setup-with-hapi` directory run the following command to add fake test data to the mock supplier:

```bash
docker compose run --rm mcsd-consumer make seed
```
