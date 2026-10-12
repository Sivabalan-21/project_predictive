"""AuthService logic with a stub repository (no database)."""
from types import SimpleNamespace

import pytest

import services.auth_service as svc_mod
from auth.security import hash_password
from services.auth_service import AuthService, InvalidUserInput, validate_new_user

PW = "correct-horse-battery-staple-42"


class StubRepo:
    def __init__(self, user=None):
        self.user = user

    def get_by_username(self, session, username):
        self.asked = username
        return self.user


@pytest.fixture()
def spies(monkeypatch):
    calls = {"dummy": 0, "verify": 0}
    real_verify = svc_mod.verify_password
    monkeypatch.setattr(svc_mod, "verify_against_dummy", lambda p: calls.__setitem__("dummy", calls["dummy"] + 1))
    monkeypatch.setattr(svc_mod, "verify_password", lambda h, p: (calls.__setitem__("verify", calls["verify"] + 1), real_verify(h, p))[1])
    return calls


def user(active=True):
    return SimpleNamespace(id=1, username="alice", role="viewer", is_active=active, password_hash=hash_password(PW))


def test_unknown_user_still_spends_an_argon2_verification(spies):
    assert AuthService(StubRepo(None)).authenticate(None, "nobody", PW) is None
    assert spies == {"dummy": 1, "verify": 0}


def test_inactive_user_is_rejected_even_with_the_right_password_and_spends_the_same_work(spies):
    assert AuthService(StubRepo(user(active=False))).authenticate(None, "alice", PW) is None
    assert spies == {"dummy": 1, "verify": 0}


def test_valid_credentials_and_wrong_password(spies):
    u = user()
    assert AuthService(StubRepo(u)).authenticate(None, "alice", PW) is u
    assert AuthService(StubRepo(u)).authenticate(None, "alice", "wrong-password-1") is None
    assert spies == {"dummy": 0, "verify": 2}


def test_username_is_normalised_before_lookup():
    repo = StubRepo(None)
    AuthService(repo).authenticate(None, "  AlIcE ", PW)
    assert repo.asked == "alice"


@pytest.mark.parametrize("username,password,role,fragment", [
    ("ab", PW, "viewer", "username"), ("alice", PW, "root", "role"), ("alice", "short", "viewer", "12-128"),
    ("alice", "xx-alice-is-in-here-xx", "viewer", "username"), ("alice", "change-me-please-now", "viewer", "too weak"),
    ("alice", "x" * 129, "viewer", "12-128")])
def test_user_policy(username, password, role, fragment):
    with pytest.raises(InvalidUserInput, match=fragment) as ei:
        validate_new_user(username, password, role)
    assert password not in str(ei.value)
    validate_new_user("alice", PW, "operator")


def test_token_is_issued_for_the_users_id_and_role():
    from auth.security import decode_access_token
    token, ttl = AuthService(StubRepo()).issue_token(SimpleNamespace(id=9, role="operator"), "s" * 40 + "abcdefghij")
    p = decode_access_token(token, "s" * 40 + "abcdefghij")
    assert (p.user_id, p.role.value, ttl) == (9, "operator", 900)
