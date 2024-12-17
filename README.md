# mCSD Consumer

This app is the mCSD (Mobile Care Service Discovery) Consumer and is part of the
gfmodules addressing stack. The purpose of this application is
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

in order to test the update mechanism, you need at least two other instances of
a FHIR store. One instance
designated a [Consumer](https://profiles.ihe.net/ITI/mCSD/volume-1.html#146113-care-services-update-consumer)
and the others are
[Providers](https://profiles.ihe.net/ITI/mCSD/volume-1.html#146113-care-services-update-consumer).
You can either use [HAPI JPA](https://hapifhir.io/hapi-fhir/),  
[addressing-register](https://github.com/minvws/gfmodules-addressing-register) or
any other FHIR store of your choosing as long as they support mCSD specs.

Follow the instructions to get the app running:

1- Setup any FHIR store of your choosing to be designated as a consumer.
2- make sure that mcsd_consumer_url is set to the base url of the store created earlier
in your app.config see ([app.conf.example](./app.conf.example)).

3- if you want to run the application by itself use the docker commands:

```bash
docker compose up
```

- Alternatively, you can have the whole stack setup from [gf-modules-coordination](https://github.com/minvws/gfmodules-coordination)

This will configure the whole system for you and you should be able to use the
API right away at <https://localhost:8509/docs>.

4- Once you reached here, make sure that you have at another instance of a
FHIR store set up to act as an update Provider.
5- You can now either use the API to add the supplier definition or install [consumer-web](https://github.com/minvws/gfmodules-mcsd-consumer-web).
Which is an admin portal for this app.

## Seeding supplier

In the case you want to seed a mock supplier with fake data, first set up the correct
URL of the mock supplier in the app.conf under `mock_seeder`.
Finally run the following command to add fake data to the mock supplier:

```bash
poetry run $(make seed)
```
