# mCSD Update Client

This app is the mCSD (Mobile Care Service Discovery) Update Client and is part of
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
a FHIR store. One instance as a [Update Client](https://profiles.ihe.net/ITI/mCSD/4.0.0-comment/volume-1.html#146113-update-client)
and at least one instance as a
[Data Source](https://profiles.ihe.net/ITI/mCSD/4.0.0-comment/volume-1.html#146114-data-source).
You can use a [HAPI JPA server](https://hapifhir.io/hapi-fhir/) or
any other FHIR store of your choosing as long as they support mCSD specs.

Follow the instructions to get the app running:

- if you want to run the application by itself, open this folder in a terminal and execute these commands:

```bash
cd example-setup-with-hapi
docker compose up
```

This will configure the whole system for you and you should be able to use the
API right away at <http://localhost:8509/docs>.

## Seeding supplier

There is a mock data seeder available in case you want to seed a mock supplier with fake data.
In a terminal in the same `example-setup-with-hapi` directory run the following command to add fake test data to the mock supplier,
with a url parameter you can specify the base url of the supplier you want to seed:

```bash
docker compose run --rm mcsd-client poetry run seed http://hapi-supplier:8080/fhir/
```
