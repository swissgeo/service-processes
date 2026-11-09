"""Change SGIDs to string (was UUID), default of process SGID is a TypeID with prefix prc

Revision ID: 3eace56772b1
Revises: a82846786150
Create Date: 2026-11-09 13:59:50.796441

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3eace56772b1"
down_revision: str | Sequence[str] | None = "a82846786150"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# TypeID (https://github.com/jetify-com/typeid): "<prefix>_<uuid as 26 char Crockford base32>".
# The UUID (128 bits) is padded with two leading zero bits to 130 bits and split in 26 groups of
# 5 bits. Implemented in SQL, as there is no TypeID support in Postgres.
CREATE_TYPEID_ENCODE = r"""
CREATE FUNCTION typeid_encode(prefix text, id uuid) RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE AS $$
DECLARE
    alphabet CONSTANT text := '0123456789abcdefghjkmnpqrstvwxyz';
    bytes bytea := uuid_send(id);
    bits text := '00'; -- bit prefix to get 130 bits (128 from uuid)
    suffix text := '';
    bit_group int;
BEGIN
    IF prefix !~ '^([a-z]([a-z_]{0,61}[a-z])?)?$' THEN
        RAISE EXCEPTION 'Invalid TypeID prefix: %', prefix;
    END IF;
    -- convert 16 bytes to 128 bits.
    FOR i IN 0..15 LOOP
        bits := bits || get_byte(bytes, i)::bit(8)::text;
    END LOOP;
    -- split 130 by 5 to get 26 characters (5 bits each)
    FOR i IN 0..25 LOOP
        bit_group := substring(bits FROM i * 5 + 1 FOR 5)::bit(5)::int;
        suffix := suffix || substr(alphabet, bit_group + 1, 1);
    END LOOP;
    RETURN CASE WHEN prefix = '' THEN suffix ELSE prefix || '_' || suffix END;
END;
$$
"""

CREATE_TYPEID_GENERATE = """
CREATE FUNCTION typeid_generate(prefix text) RETURNS text
LANGUAGE sql VOLATILE PARALLEL SAFE AS $$ SELECT typeid_encode(prefix, uuidv7()) $$
"""


def upgrade() -> None:
    """Upgrade schema.

    Existing SGIDs are kept as they are, only new processes get a TypeID.
    """
    op.alter_column(
        "process",
        "sgid",
        existing_type=sa.UUID(),
        type_=sa.String(length=128),
        existing_nullable=False,
        existing_server_default=sa.text("uuidv7()"),
    )
    op.alter_column(
        "process",
        "organization_sgid",
        existing_type=sa.UUID(),
        type_=sa.String(length=128),
        existing_nullable=False,
    )
    op.alter_column(
        "process",
        "organization_unit_sgid",
        existing_type=sa.UUID(),
        type_=sa.String(length=128),
        existing_nullable=False,
    )
    op.alter_column(
        "process",
        "dataset_sgid",
        existing_type=sa.UUID(),
        type_=sa.String(length=128),
        existing_nullable=False,
    )

    op.execute(CREATE_TYPEID_ENCODE)
    op.execute(CREATE_TYPEID_GENERATE)
    op.alter_column(
        "process",
        "sgid",
        existing_type=sa.String(length=128),
        existing_nullable=False,
        server_default=sa.text("typeid_generate('prc')"),
    )


def downgrade() -> None:
    """Downgrade schema.

    Fails if any SGID that is not a valid UUID (e.g. a TypeID) has been stored in the meantime.
    """
    # The default can't be cast automatically from varchar to uuid, so drop and recreate it
    op.alter_column("process", "sgid", existing_type=sa.String(length=128), server_default=None)
    op.execute("DROP FUNCTION typeid_generate(text)")
    op.execute("DROP FUNCTION typeid_encode(text, uuid)")

    op.alter_column(
        "process",
        "dataset_sgid",
        existing_type=sa.String(length=128),
        type_=sa.UUID(),
        existing_nullable=False,
        postgresql_using="dataset_sgid::uuid",
    )
    op.alter_column(
        "process",
        "organization_unit_sgid",
        existing_type=sa.String(length=128),
        type_=sa.UUID(),
        existing_nullable=False,
        postgresql_using="organization_unit_sgid::uuid",
    )
    op.alter_column(
        "process",
        "organization_sgid",
        existing_type=sa.String(length=128),
        type_=sa.UUID(),
        existing_nullable=False,
        postgresql_using="organization_sgid::uuid",
    )
    op.alter_column(
        "process",
        "sgid",
        existing_type=sa.String(length=128),
        type_=sa.UUID(),
        existing_nullable=False,
        postgresql_using="sgid::uuid",
    )
    op.alter_column("process", "sgid", existing_type=sa.UUID(), server_default=sa.text("uuidv7()"))
