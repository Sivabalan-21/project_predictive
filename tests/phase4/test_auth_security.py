from datetime import datetime, timedelta, timezone

import jwt
import pytest

from auth.security import (ACCESS_TOKEN_TTL, ALGORITHM, ISSUER, Role, TokenError, create_access_token, decode_access_token,
                           hash_password, verify_password, verify_against_dummy)

SECRET = "t3st-signing-secret-with-more-than-32-chars!!"


def _claims(**over):
    now = datetime.now(timezone.utc)
    base = {"iss": ISSUER, "sub": "7", "role": "viewer", "iat": now, "exp": now + timedelta(minutes=10)}
    base.update({k: v for k, v in over.items() if v is not None})
    return {k: v for k, v in base.items() if over.get(k, 0) != "DROP"}


def test_ttl_is_fifteen_minutes_and_algorithm_hs256():
    assert ACCESS_TOKEN_TTL == timedelta(minutes=15) and ALGORITHM == "HS256"
    token, expires_in = create_access_token(7, Role.OPERATOR, SECRET)
    assert expires_in == 900
    claims = jwt.decode(token, SECRET, algorithms=["HS256"], issuer=ISSUER)
    assert claims["exp"] - claims["iat"] == 900 and claims["sub"] == "7" and claims["role"] == "operator" and claims["jti"]
    assert jwt.get_unverified_header(token)["alg"] == "HS256"


@pytest.mark.parametrize("role", list(Role))
def test_roundtrip(role):
    token, _ = create_access_token(42, role, SECRET)
    p = decode_access_token(token, SECRET)
    assert p.user_id == 42 and p.role is role


def test_role_hierarchy():
    from auth.security import Principal
    assert Principal(1, Role.VIEWER).allows(Role.VIEWER) and not Principal(1, Role.VIEWER).allows(Role.OPERATOR)
    assert Principal(1, Role.OPERATOR).allows(Role.VIEWER) and Principal(1, Role.OPERATOR).allows(Role.OPERATOR)
    assert not Principal(1, Role.OPERATOR).allows(Role.ADMIN)
    assert all(Principal(1, Role.ADMIN).allows(r) for r in Role)


def test_expired_token_rejected():
    token, _ = create_access_token(1, "viewer", SECRET, now=datetime.now(timezone.utc) - timedelta(minutes=16))
    with pytest.raises(TokenError):
        decode_access_token(token, SECRET)


def test_token_just_inside_ttl_is_accepted():
    token, _ = create_access_token(1, "viewer", SECRET, now=datetime.now(timezone.utc) - timedelta(minutes=14))
    assert decode_access_token(token, SECRET).user_id == 1


def test_lifetime_longer_than_fifteen_minutes_is_rejected_even_with_a_valid_signature():
    token, _ = create_access_token(1, "viewer", SECRET, ttl=timedelta(hours=24))
    with pytest.raises(TokenError, match="15 minute"):
        decode_access_token(token, SECRET)


def test_wrong_secret_and_tampering_rejected():
    token, _ = create_access_token(1, "viewer", SECRET)
    with pytest.raises(TokenError):
        decode_access_token(token, SECRET + "x")
    head, body, sig = token.split(".")
    forged = jwt.encode({**jwt.decode(token, SECRET, algorithms=["HS256"], issuer=ISSUER), "role": "admin"}, "other-secret-other-secret-other-secret-1", algorithm="HS256")
    with pytest.raises(TokenError):
        decode_access_token(forged, SECRET)
    with pytest.raises(TokenError):
        decode_access_token(f"{head}.{body}.{sig[:-2]}AA", SECRET)


def test_alg_none_and_other_algorithms_rejected():
    unsigned = jwt.encode(_claims(), key=None, algorithm="none")
    with pytest.raises(TokenError):
        decode_access_token(unsigned, SECRET)
    hs512 = jwt.encode(_claims(), SECRET, algorithm="HS512")
    with pytest.raises(TokenError):
        decode_access_token(hs512, SECRET)


@pytest.mark.parametrize("missing", ["exp", "iat", "sub", "role", "iss"])
def test_required_claims_enforced(missing):
    claims = _claims()
    claims.pop(missing)
    with pytest.raises(TokenError):
        decode_access_token(jwt.encode(claims, SECRET, algorithm="HS256"), SECRET)


def test_wrong_issuer_unknown_role_bad_subject_rejected():
    for over in ({"iss": "someone-else"}, {"role": "superuser"}, {"sub": "not-a-number"}):
        with pytest.raises(TokenError):
            decode_access_token(jwt.encode(_claims(**over), SECRET, algorithm="HS256"), SECRET)


@pytest.mark.parametrize("junk", ["", "abc", "a.b.c", "Bearer x", "\x00"])
def test_garbage_tokens_rejected(junk):
    with pytest.raises(TokenError):
        decode_access_token(junk, SECRET)


def test_argon2id_hashing_salted_and_verifiable():
    h1, h2 = hash_password("correct-horse-battery"), hash_password("correct-horse-battery")
    assert h1.startswith("$argon2id$") and h1 != h2 and "correct-horse" not in h1
    assert verify_password(h1, "correct-horse-battery") and not verify_password(h1, "wrong")
    assert not verify_password("not-a-hash", "x") and not verify_password("", "x")
    verify_against_dummy("anything")             # must not raise
