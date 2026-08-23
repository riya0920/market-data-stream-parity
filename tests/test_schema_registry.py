"""Schema compatibility, in both directions and at its limit."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.schema_registry import (Field, IncompatibleSchema, Registry, Schema,
                                 check_compatibility)


def _schema(fields, compat="BACKWARD"):
    return Schema("trade", 0, fields, compatibility=compat)


BASE = [Field("symbol", "string"), Field("price", "float"),
        Field("size", "float")]


# ---------------------------------------------------------------- backward
def test_adding_an_optional_field_is_backward_compatible():
    new = _schema(BASE + [Field("venue", "string", required=False,
                                default="unknown")])
    assert check_compatibility(_schema(BASE), new, "BACKWARD") == []


def test_adding_a_required_field_is_not_backward_compatible():
    """A new reader cannot find it in the archive."""
    new = _schema(BASE + [Field("venue", "string")])
    assert check_compatibility(_schema(BASE), new, "BACKWARD")


def test_making_an_existing_field_required_is_not_backward_compatible():
    old = _schema(BASE + [Field("venue", "string", required=False, default="x")])
    new = _schema(BASE + [Field("venue", "string")])
    assert check_compatibility(old, new, "BACKWARD")


def test_dropping_a_field_is_backward_compatible():
    assert check_compatibility(_schema(BASE), _schema(BASE[:2]), "BACKWARD") == []


# ----------------------------------------------------------------- forward
def test_adding_a_required_field_IS_forward_compatible():
    """The asymmetry: an old reader ignores what it does not know about."""
    new = _schema(BASE + [Field("venue", "string")])
    assert check_compatibility(_schema(BASE), new, "FORWARD") == []


def test_dropping_a_required_field_is_not_forward_compatible():
    assert check_compatibility(_schema(BASE), _schema(BASE[:2]), "FORWARD")


def test_full_requires_both_and_refuses_what_either_refuses():
    add_required = _schema(BASE + [Field("venue", "string")])
    assert check_compatibility(_schema(BASE), add_required, "FULL")
    assert check_compatibility(_schema(BASE), _schema(BASE[:2]), "FULL")


def test_none_permits_anything_which_is_what_this_project_had():
    assert check_compatibility(_schema(BASE), _schema([Field("x", "int")]),
                               "NONE") == []


# ------------------------------------------------------------------- types
def test_renaming_a_field_is_refused_in_every_mode():
    renamed = [Field("symbol", "string"), Field("px", "float"),
               Field("size", "float")]
    for mode in ("BACKWARD", "FORWARD", "FULL"):
        assert check_compatibility(_schema(BASE), _schema(renamed), mode)


def test_widening_int_to_float_is_backward_but_not_forward():
    old = _schema([Field("size", "int")])
    new = _schema([Field("size", "float")])
    assert check_compatibility(old, new, "BACKWARD") == []
    assert check_compatibility(old, new, "FORWARD")


def test_narrowing_float_to_int_passes_forward_and_that_is_the_limit():
    """It came out against what I expected, and the check is right: an old
    reader expecting a float accepts an integer. Nothing about the
    REPRESENTATION breaks -- only the value, which is truncated at the producer.
    Compatibility is an algebra over types; truncation is about meaning."""
    old = _schema([Field("price", "float")])
    new = _schema([Field("price", "int")])
    assert check_compatibility(old, new, "FORWARD") == []
    assert check_compatibility(old, new, "BACKWARD")


# ---------------------------------------------------------------- registry
def test_a_breaking_change_raises_at_registration_not_at_read():
    reg = Registry()
    reg.register(_schema(BASE))
    with pytest.raises(IncompatibleSchema):
        reg.register(_schema([Field("symbol", "string"), Field("px", "float")]))


def test_a_compatible_change_bumps_the_version():
    reg = Registry()
    reg.register(_schema(BASE))
    v = reg.register(_schema(BASE + [Field("venue", "string", required=False,
                                           default="x")]))
    assert v == 2 and reg.latest("trade").version == 2


def test_a_producer_cannot_loosen_the_mode_in_the_same_request_that_breaks_it():
    """The mode comes from the CURRENT schema unless explicitly overridden."""
    reg = Registry()
    reg.register(_schema(BASE, compat="BACKWARD"))
    breaking = _schema(BASE + [Field("venue", "string")], compat="NONE")
    with pytest.raises(IncompatibleSchema):
        reg.register(breaking)


def test_the_version_travels_with_the_record():
    reg = Registry()
    reg.register(_schema(BASE))
    env = reg.envelope("trade", {"symbol": "BTC", "price": 1.0, "size": 2.0})
    assert env["__version"] == 1 and env["__subject"] == "trade"


def test_a_record_missing_a_required_field_is_refused():
    reg = Registry()
    reg.register(_schema(BASE))
    with pytest.raises(IncompatibleSchema, match="missing required"):
        reg.envelope("trade", {"symbol": "BTC", "size": 2.0})


def test_a_record_with_the_wrong_type_is_refused():
    reg = Registry()
    reg.register(_schema(BASE))
    with pytest.raises(IncompatibleSchema, match="expected"):
        reg.envelope("trade", {"symbol": "BTC", "price": "cheap", "size": 2.0})


def test_history_is_read_under_the_schema_that_wrote_it():
    """The usual guess is 'the latest', which is wrong for every record written
    before the last change -- and a replay is when that guess costs something."""
    reg = Registry()
    reg.register(_schema(BASE))
    old = reg.envelope("trade", {"symbol": "BTC", "price": 1.0, "size": 2.0})
    reg.register(_schema(BASE + [Field("venue", "string", required=False,
                                       default="x")]))
    rec, schema = reg.read(old)
    assert schema.version == 1
    assert "venue" not in rec


def test_an_unstamped_record_is_readable_and_says_so():
    """Records written before the registry existed have no stamp. Refusing them
    would make the registry unadoptable on a live feed."""
    rec, schema = Registry().read({"symbol": "BTC", "price": 1.0})
    assert schema is None and rec == {"symbol": "BTC", "price": 1.0}
