# mCSD Update Client

This app is the mCSD (Mobile Care Service Discovery) Update Client and is part of
the 'Generieke Functies, lokalisatie en addressering' project of the Ministry of Health, Welfare and Sport of the Dutch government. The purpose of this application is
to perform updates on [mCSD Supported Resources](https://profiles.ihe.net/ITI/mCSD/index.html). The update uses HTTP as a basis for CRUD operations and operates regardless of
the FHIR store type.

> [!CAUTION]
> This project is a **reference implementation** for the [ITI-91: Request Care Services Update](https://profiles.ihe.net/ITI/mCSD/ITI-91.html) transaction.

## Reference Implementation

This codebase serves as an example of how the mCSD ITI-91 update mechanism can be implemented. It is **NOT** intended for production use.

This application does NOT process, store, or handle any Personally Identifiable Information (PII).
All data used for testing or demonstration is synthetic or non-sensitive.

### Purpose

- Demonstrate mCSD system communication patterns
- Support documentation and proof-of-concept development
- Enable basic interoperability testing

### Use Cases

Use this to:

- Understand how the mCSD updates work in practice
- Explore Update Client and Data Source interactions
- Prototype or test mCSD-compliant behaviors

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

## Seeding directory

There is a mock data seeder available in case you want to seed a mock directory with fake data.
In a terminal in the same `example-setup-with-hapi` folder run the following command to add fake test data to the mock directory,
with a url parameter you can specify the base url of the directory you want to seed:

```bash
docker compose run --rm mcsd-update-client poetry run seed http://hapi-directory:8080/fhir/
```

## Docker container builds

There are two ways to build a docker container from this application. The first is the default mode created with:

    make container-build

This will build a docker container that will run its migrations to the database specified in app.conf.

The second mode is a "standalone" mode, where it will not generate migrations, and where you must explicitly specify
an app.conf mount.

    make container-build-sa

Both containers only differ in their init script and the default version usually will mount its own local src directory
into the container's /src dir.

## URL resolving of references

When encountering absolute URLS in references, they will be resolved ONLY when the URL is of the same origin. Otherwise
it will throw an exception.

## Contribution

As stated in the [Disclaimer](#disclaimer) this project and all associated code serve solely as documentation and 
demonstration purposes to illustrate potential system communication patterns and architectures.

For that reason we will only accept contributions that fit this goal. We do appreciate any effort from the 
community, but because our time is limited it is possible that your PR or issue is closed without a full justification.

If you plan to make non-trivial changes, we recommend to open an issue beforehand where we can discuss your planned changes. This increases the chance that we might be able to use your contribution (or it avoids doing work if there are reasons why we wouldn't be able to use it).

Note that all commits should be signed using a gpg key.
