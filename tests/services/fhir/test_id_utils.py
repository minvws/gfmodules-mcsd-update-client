from app.services.fhir.id_utils import make_namespaced_fhir_id


def test_make_namespaced_fhir_id_keeps_short_ids() -> None:
    assert make_namespaced_fhir_id("dir", "res") == "dir-res"


def test_make_namespaced_fhir_id_hashes_long_ids_to_64_chars() -> None:
    namespace = "091f4442-a586-4835-9db5-8c77ccf3cd11"
    resource_id = "16d53ce6-6354-434d-8748-88c3b1992bf4"
    out = make_namespaced_fhir_id(namespace, resource_id)
    assert len(out) == 64
    assert out.islower()
    assert all(c in "0123456789abcdef" for c in out)
