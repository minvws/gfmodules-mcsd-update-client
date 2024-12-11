# mCSD Consumer

This app is the mCSD Consumer and is part of the gfmodules addressing stack.

## First run

If you need to run the application without actual development, you can use the autopilot functionality. When this
repository is checked out, just run the following command:

```bash
make autopilot
```

This will configure the whole system for you and you should be able to use the API right away at https://localhost:8009/docs

## Seeding supplier

In the case you want to seed a mock supplier with fake data, first set up the correct URL of the mock supplier in the app.conf under `mock_seeder`.
Finally run the following command to add fake data to the mock supplier:

```bash
poetry run $(make seed)
```
