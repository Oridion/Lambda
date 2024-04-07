from __future__ import annotations
import typing
from solders.pubkey import Pubkey
from solders.instruction import Instruction, AccountMeta
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class PlanetHopArgs(typing.TypedDict):
    lamports: int


layout = borsh.CStruct("lamports" / borsh.U64)


class PlanetHopAccounts(typing.TypedDict):
    to: Pubkey
    from_: Pubkey
    manager: Pubkey


def planet_hop(
    args: PlanetHopArgs,
    accounts: PlanetHopAccounts,
    program_id: Pubkey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> Instruction:
    keys: list[AccountMeta] = [
        AccountMeta(pubkey=accounts["to"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["from"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["manager"], is_signer=True, is_writable=True),
    ]
    if remaining_accounts is not None:
        keys += remaining_accounts
    identifier = b"\x03\xb3\xa4\xb7\xedq\x9d\x86"
    encoded_args = layout.build(
        {
            "lamports": args["lamports"],
        }
    )
    data = identifier + encoded_args
    return Instruction(program_id, data, keys)
