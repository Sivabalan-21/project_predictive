import io
import subprocess
import sys

import pytest
from sqlalchemy import text

from auth.cli import build_parser, main, read_password
from auth.security import verify_password
from tests.phase4.conftest import ROOT, STRONG_PASSWORD


class Tty(io.StringIO):
    def isatty(self):
        return True


# ----------------------------------------------------------------------------- the password can never be an argument
@pytest.mark.parametrize("argv", [
    ["create-user", "--username", "alice", "--password", "pw-from-argv-1234"],
    ["create-user", "--username", "alice", "--password=pw-from-argv-1234"],
    ["create-user", "--username", "alice", "-p", "pw-from-argv-1234"],
    ["create-user", "--username", "alice", "--pass", "pw-from-argv-1234"],       # abbreviation of --password-stdin must NOT work
    ["create-user", "--username", "alice", "--password-std"],
    ["create-user", "--username", "alice", "pw-from-argv-1234"],                 # positional
])
def test_password_on_the_command_line_is_rejected_by_the_parser(argv, capsys):
    with pytest.raises(SystemExit) as ei:
        build_parser().parse_args(argv)
    assert ei.value.code == 2


def test_cli_exposes_no_password_value_option():
    ap = build_parser()
    sub = next(a for a in ap._actions if a.choices).choices["create-user"]
    options = {o for a in sub._actions for o in a.option_strings}
    assert options == {"-h", "--help", "--username", "--role", "--password-stdin"}
    assert [a.nargs for a in sub._actions if "--password-stdin" in a.option_strings] == [0]      # a flag: takes no value



def test_password_argument_is_rejected_in_a_real_subprocess_and_never_echoed():
    import os

    env = os.environ.copy()
    # Keep the current Python environment available on Windows and Linux.
    p = subprocess.run(
        [
            sys.executable, "-m", "auth.cli",
            "create-user", "--username", "alice",
            "--password", "SuperSecret-9876",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=env,
    )
    assert p.returncode == 2
    assert "unrecognized arguments" in p.stderr
    assert "SuperSecret-9876" not in p.stdout
    assert "SuperSecret-9876" not in p.stderr



# ----------------------------------------------------------------------------- reading the password
def test_stdin_password_reads_one_line_and_strips_the_newline():
    assert read_password(True, io.StringIO("pw-line-one-123\nsecond-line\n")) == "pw-line-one-123"
    assert read_password(True, io.StringIO("no-newline-at-all")) == "no-newline-at-all"


def test_empty_stdin_is_an_error():
    with pytest.raises(ValueError, match="no password"):
        read_password(True, io.StringIO(""))


def test_without_a_terminal_or_the_flag_it_refuses_instead_of_hanging():
    with pytest.raises(ValueError, match="--password-stdin"):
        read_password(False, io.StringIO("ignored"))


def test_interactive_prompt_asks_twice_and_requires_a_match(monkeypatch):
    answers = iter(["same-password-123", "same-password-123"])
    monkeypatch.setattr("auth.cli.getpass.getpass", lambda prompt="": next(answers))
    assert read_password(False, Tty()) == "same-password-123"
    answers2 = iter(["one-password-123", "another-password-123"])
    monkeypatch.setattr("auth.cli.getpass.getpass", lambda prompt="": next(answers2))
    with pytest.raises(ValueError, match="do not match"):
        read_password(False, Tty())


# ----------------------------------------------------------------------------- end to end against real PostgreSQL
@pytest.mark.integration
@pytest.mark.postgres
class TestCliWithPostgres:
    def run(self, migrated_url, capsys, *argv, stdin=STRONG_PASSWORD + "\n"):
        code = main(list(argv), stdin=io.StringIO(stdin))
        out = capsys.readouterr()
        return code, out.out + out.err

    def test_create_user_stores_an_argon2id_hash_never_the_password(self, migrated_url, session_factory, capsys):
        code, output = self.run(migrated_url, capsys, "create-user", "--username", "Cli-User-1", "--role", "operator", "--password-stdin")
        assert code == 0 and "'cli-user-1'" in output and "operator" in output
        assert STRONG_PASSWORD not in output and "argon2" not in output
        with session_factory() as s:
            row = s.execute(text("SELECT username, role, is_active, password_hash FROM users WHERE username='cli-user-1'")).one()
        assert row.role == "operator" and row.is_active is True
        assert row.password_hash.startswith("$argon2id$") and STRONG_PASSWORD not in row.password_hash
        assert verify_password(row.password_hash, STRONG_PASSWORD)

    def test_duplicate_user_exits_1_and_keeps_the_original_hash(self, migrated_url, session_factory, capsys):
        assert self.run(migrated_url, capsys, "create-user", "--username", "cli-dup", "--password-stdin")[0] == 0
        code, output = self.run(migrated_url, capsys, "create-user", "--username", "cli-dup", "--password-stdin",
                                stdin="a-different-password-77\n")
        assert code == 1 and "already exists" in output and "a-different-password-77" not in output
        with session_factory() as s:
            h = s.execute(text("SELECT password_hash FROM users WHERE username='cli-dup'")).scalar_one()
        assert verify_password(h, STRONG_PASSWORD) and not verify_password(h, "a-different-password-77")

    @pytest.mark.parametrize("pw,fragment", [("short", "12-128"), ("x" * 129, "12-128"), ("pw-short-1", "12-128"),
                                             ("change-me-please-now", "too weak"), ("xx-cli-weak-xx-1234", "username")])
    def test_policy_violations_exit_1_without_creating_anything(self, migrated_url, session_factory, capsys, pw, fragment):
        code, output = self.run(migrated_url, capsys, "create-user", "--username", "cli-weak", "--password-stdin", stdin=pw + "\n")
        assert code == 1 and fragment in output and pw not in output
        with session_factory() as s:
            assert s.execute(text("SELECT count(*) FROM users WHERE username='cli-weak'")).scalar_one() == 0

    @pytest.mark.parametrize("name", ["ab", "-bad", "Has Space", "a" * 65, "semi;colon"])
    def test_bad_usernames_exit_1(self, migrated_url, capsys, name):
        code, output = self.run(migrated_url, capsys, "create-user", f"--username={name}", "--password-stdin")
        assert code == 1 and "username must be" in output

    def test_unknown_role_is_rejected_by_the_parser(self, migrated_url):
        with pytest.raises(SystemExit) as ei:
            main(["create-user", "--username", "cli-role", "--role", "superuser", "--password-stdin"], stdin=io.StringIO("x\n"))
        assert ei.value.code == 2

    def test_missing_database_url_exits_2(self, monkeypatch, capsys):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        assert main(["create-user", "--username", "cli-nodb", "--password-stdin"], stdin=io.StringIO(STRONG_PASSWORD + "\n")) == 2
        assert "DATABASE_URL" in capsys.readouterr().err

    def test_production_refuses_a_placeholder_database_password(self, monkeypatch, capsys):
        monkeypatch.setenv("APP_ENV", "production")
        monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:change-me@127.0.0.1:1/db")
        assert main(["create-user", "--username", "cli-prod", "--password-stdin"], stdin=io.StringIO(STRONG_PASSWORD + "\n")) == 2
        assert "placeholder" in capsys.readouterr().err

    def test_unmigrated_database_gives_a_clear_error_exit_3(self, scratch_db_url, monkeypatch, capsys):
        monkeypatch.setenv("DATABASE_URL", scratch_db_url)           # empty database: no users table yet
        code = main(["create-user", "--username", "cli-nomig", "--password-stdin"], stdin=io.StringIO(STRONG_PASSWORD + "\n"))
        assert code == 3 and "alembic upgrade head" in capsys.readouterr().err
