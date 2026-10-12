"""Create API users.   python -m auth.cli create-user --username alice --role operator

The password is NEVER accepted as a command-line argument (it would end up in shell history and in ``ps`` output):

* interactive terminal: prompted twice with echo disabled (``getpass``);
* automation:           ``--password-stdin`` reads ONE line from standard input, e.g.
                        ``printf '%s' "$PW" | python -m auth.cli create-user --username svc --role viewer --password-stdin``

Exit codes: 0 created | 1 rejected (policy violation or user already exists) | 2 usage / configuration error | 3 database error.
Needs ``DATABASE_URL`` (and honours ``APP_ENV``). The password and its hash are never printed or logged.
"""
from __future__ import annotations

import argparse
import getpass
import sys
import re
from typing import Sequence, TextIO

from sqlalchemy.exc import SQLAlchemyError

from auth.security import Role
from core.settings import AppSettings
from db.config import ConfigurationError, DatabaseSettings
from db.session import make_engine, make_session_factory
from services.auth_service import AuthService, InvalidUserInput
from services.errors import ConflictError


class SafeArgumentParser(argparse.ArgumentParser):
    """Avoid echoing rejected password arguments in parser errors."""

    def error(self, message: str) -> None:
        message = re.sub(
            r"(--password)(?:\s+|=)\S+",
            r"\1 [REDACTED]",
            message,
        )
        super().error(message)



def build_parser() -> argparse.ArgumentParser:
    # Do not echo rejected password values in parser errors.
    ap = SafeArgumentParser(
        prog="python -m auth.cli",
        description="Manage API users.",
        allow_abbrev=False,
    )
    sub = ap.add_subparsers(
        dest="command",
        required=True,
        parser_class=SafeArgumentParser,
    )
    cu = sub.add_parser(
        "create-user",
        help="create a user (password via prompt or --password-stdin)",
        allow_abbrev=False,
    )
    cu.add_argument("--username", required=True)
    cu.add_argument("--role", choices=[r.value for r in Role], default=Role.VIEWER.value)
    cu.add_argument("--password-stdin", action="store_true", help="read the password from stdin (one line)")
    return ap



def read_password(from_stdin: bool, stdin: TextIO = sys.stdin) -> str:
    if from_stdin:
        line = stdin.readline()
        if not line:
            raise ValueError("no password received on stdin")
        return line.rstrip("\r\n")
    if not stdin.isatty():
        raise ValueError("no interactive terminal: pass --password-stdin and pipe the password in")
    first = getpass.getpass("Password: ")
    if first != getpass.getpass("Repeat password: "):
        raise ValueError("passwords do not match")
    return first


def main(argv: Sequence[str] | None = None, stdin: TextIO | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        AppSettings.from_env(require_jwt=False)                    # APP_ENV validity + production placeholder checks
        db_settings = DatabaseSettings.from_env()
        password = read_password(args.password_stdin, stdin or sys.stdin)
    except (ConfigurationError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    engine = make_engine(db_settings)
    try:
        with make_session_factory(engine)() as session:
            user = AuthService().create_user(session, args.username, password, args.role)
        print(f"created user {user.username!r} with role {user.role!r}")
        return 0
    except (InvalidUserInput, ConflictError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except SQLAlchemyError as exc:
        print(f"error: database error ({exc.__class__.__name__}); is the schema migrated (alembic upgrade head)?",
              file=sys.stderr)
        return 3
    finally:
        engine.dispose()


if __name__ == "__main__":
    sys.exit(main())
