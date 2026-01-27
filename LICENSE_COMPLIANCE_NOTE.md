# License Compliance Note

Date: 2026-01-26

## Requirement (PvA)
- Source code must be licensed under MIT.
- Documentation must be licensed under CC BY-SA 4.0.

## Current repo status
- Root code license: `LICENSE` is MIT.
- Documentation license text: `LICENSES/CC-BY-SA-4.0.txt` added.
- `services/iti-130`:
  - Code: MIT (`services/iti-130/LICENSE.md`).
  - Docs: CC BY-SA 4.0 (noted in `services/iti-130/README.md`).
- `services/iti-90`:
  - Code: MIT (`services/iti-90/LICENSE.md`).
  - Docs: CC BY-SA 4.0 (noted in `services/iti-90/README.md`).
- `services/iti-91`:
  - Code: EUPL-1.2 (`services/iti-91/LICENSE.md`, `services/iti-91/REUSE.toml`, `services/iti-91/pyproject.toml`).
  - Docs: CC BY-SA 4.0 (`services/iti-91/LICENSE.md`, `services/iti-91/REUSE.toml`).

## Mismatches / exceptions
- `services/iti-91` code is EUPL-1.2, not MIT. This conflicts with the PvA requirement unless an explicit exception/approval exists.
- Third-party dependencies are under various licenses (e.g., Apache-2.0, BSD, MPL-2.0, LGPL-3.0-only). These do not change your own code license, but you must comply with their terms.

## Third-party license inventories
- `services/iti-130/THIRD_PARTY_LICENSES.md`
- `services/iti-90/THIRD_PARTY_LICENSES.md`
- `services/iti-91/THIRD_PARTY_LICENSES.md`

## Recommended actions to reach full compliance
- If MIT-only delivery is required, replace or remove `services/iti-91` or obtain permission to relicense.
- Keep documentation notices pointing to CC BY-SA 4.0.
- Maintain third-party license lists and include notices in distributions.
