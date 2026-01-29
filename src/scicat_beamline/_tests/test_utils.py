import json
import numpy as np

from scicat_beamline.utils import NPArrayEncoder, clean_email, UNKNOWN_EMAIL, search_terms_from_name, calculate_access_controls, Issue


def test_clean_email_valid():
    # Remove surrounding whitespace.
    assert clean_email("  user@example.com  ") == "user@example.com"


def test_clean_email_none():
    # Non-string input returns the default unknown email.
    assert clean_email(None) == UNKNOWN_EMAIL


def test_clean_email_empty():
    # An empty string (or only spaces) should return the default.
    assert clean_email("   ") == UNKNOWN_EMAIL


def test_clean_email_no_at_symbol():
    # A string without an "@" should return the default.
    assert clean_email("invalid-email") == UNKNOWN_EMAIL


def test_clean_email_literal_none():
    # The string "NONE" (case-insensitive) should return the default.
    assert clean_email("NONE") == UNKNOWN_EMAIL
    assert clean_email("none") == UNKNOWN_EMAIL


def test_clean_email_internal_spaces():
    # Spaces inside the email should be removed.
    # For example, " user @ example.com " should be cleaned to "user@example.com"
    assert clean_email(" user @ example.com ") == "user@example.com"


def test_np_encoder():
    test_dict = {"dont_panic": np.array([1, 2, 3], dtype=np.int8)}
    assert json.dumps(test_dict, cls=NPArrayEncoder)

    test_dict = {"dont_panic": np.array([1, 2, 3], dtype=np.float32)}
    assert json.dumps(test_dict, cls=NPArrayEncoder)

    test_dict = {"dont_panic": np.full((1, 1), np.inf)}
    encoded_np = json.loads(json.dumps(test_dict, cls=NPArrayEncoder))
    assert json.dumps(encoded_np, allow_nan=False)


def test_search_terms_from_name():
    terms = search_terms_from_name("Time-is_an illusion. Lunchtime/2x\\so.")
    assert "time" in terms
    assert "is" in terms
    assert "an" in terms
    assert "illusion" in terms
    assert "lunchtime" in terms
    assert "2x" in terms
    assert "so" in terms


def test_access_controls():
    username = "slartibartfast"
    # no proposal, no beamline
    access_controls = calculate_access_controls(username, None, None)
    assert access_controls["owner_group"] == "slartibartfast"
    assert access_controls["access_groups"] == []

    # proposal and no beamline
    access_controls = calculate_access_controls(username, None, "42")
    assert access_controls["owner_group"] == "42"

    # no proposal and beamline
    access_controls = calculate_access_controls(username, "10.3.1", None)
    assert access_controls["owner_group"] == "slartibartfast"
    assert "10.3.1" in access_controls["access_groups"]
    assert "slartibartfast" in access_controls["access_groups"]

    # proposal and beamline
    access_controls = calculate_access_controls(username, "10.3.1", "42")
    assert access_controls["owner_group"] == "42"
    assert "10.3.1" in access_controls["access_groups"]

    # special 8.3.2 mapping
    access_controls = calculate_access_controls(username, "bl832", "42")
    assert access_controls["owner_group"] == "42"
    assert "8.3.2" in access_controls["access_groups"]
    assert "bl832" in access_controls["access_groups"]


def test_clean_email():
    pass
