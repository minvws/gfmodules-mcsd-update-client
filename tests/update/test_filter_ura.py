from typing import List

import pytest
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.organization import Organization
from fhir.resources.R4B.reference import Reference


@pytest.fixture
def orgs() -> List[Organization]:
    return [
        Organization(
            id="test-org-12345",
            identifier=[
                Identifier(
                    system="http://fhir.nl/fhir/NamingSystem/ura",
                    value="12345678",
                )
            ],
            name="Example Organization",
            endpoint=[
                Reference(
                    reference="Endpoint/endpoint-test1",
                )
            ],
        ),
        Organization(
            id="test-org-67890",
            identifier=[
                Identifier(
                    system="http://fhir.nl/fhir/NamingSystem/ura",
                    value="112233",
                ),
                Identifier(
                    system="http://some/other/system",
                    value="112233",
                ),
                Identifier(
                    system="http://fhir.nl/fhir/NamingSystem/ura",
                    value="325252",
                ),
                Identifier(
                    system="http://some/another/system",
                    value="325252",
                ),
            ],
            name="Another Organization",
            endpoint=[
                Reference(
                    reference="Endpoint/endpoint-test2",
                ),
            ],
        )
    ]


def test_filter_ura(orgs: List[Organization]) -> None:
    from app.services.update.filter_ura import filter_ura

    # Nothing filtered
    ura_whitelist = ["12345678", "325252"]
    res = filter_ura(orgs[0], ura_whitelist)
    if res.identifier is None:
        res.identifier = []
    assert len(res.identifier) == 1

    # filter identifier
    ura_whitelist = ["1111", "2222"]
    res = filter_ura(orgs[0], ura_whitelist)
    if res.identifier is None:
        res.identifier = []
    assert len(res.identifier) == 0


def test_filter_ura_with_multiple_ids(orgs: List[Organization]) -> None:
    from app.services.update.filter_ura import filter_ura

    # Filter only last id
    ura_whitelist = ["11111", "325252"]
    res = filter_ura(orgs[1], ura_whitelist)
    if res.identifier is None:
        res.identifier = []
    assert len(res.identifier) == 3
    assert res.identifier[0].value == "112233"
    assert res.identifier[1].value == "325252"
    assert res.identifier[2].value == "325252"

    # Filter all ura, but keep others
    ura_whitelist = []
    res = filter_ura(orgs[1], ura_whitelist)
    if res.identifier is None:
        res.identifier = []
    assert len(res.identifier) == 2
    assert res.identifier[0].value == "112233"
    assert res.identifier[1].value == "325252"

    # All ura's are valid
    ura_whitelist = ["112233", "325252"]
    res = filter_ura(orgs[1], ura_whitelist)
    if res.identifier is None:
        res.identifier = []
    assert len(res.identifier) == 4
    assert res.identifier[0].value == "112233"
    assert res.identifier[1].value == "112233"
    assert res.identifier[2].value == "325252"
    assert res.identifier[3].value == "325252"
