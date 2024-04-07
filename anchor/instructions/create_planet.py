from __future__ import annotations
import typing
from solders.pubkey import Pubkey
from solders.system_program import ID as SYS_PROGRAM_ID
from solders.sysvar import RENT
from solders.instruction import Instruction, AccountMeta
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class CreatePlanetArgs(typing.TypedDict):
    name: str


layout = borsh.CStruct("name" / borsh.String)


class CreatePlanetAccounts(typing.TypedDict):
    planet: Pubkey
    universe: Pubkey
    creator: Pubkey


def create_planet(
    args: CreatePlanetArgs,
    accounts: CreatePlanetAccounts,
    program_id: Pubkey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> Instruction:
    keys: list[AccountMeta] = [
        AccountMeta(pubkey=accounts["planet"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["universe"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["creator"], is_signer=True, is_writable=True),
        AccountMeta(pubkey=SYS_PROGRAM_ID, is_signer=False, is_writable=False),
        AccountMeta(pubkey=RENT, is_signer=False, is_writable=False),
    ]
    if remaining_accounts is not None:
        keys += remaining_accounts
    identifier = b"m*\x00\x84\x91\xd2\x0f\x19"
    encoded_args = layout.build(
        {
            "name": args["name"],
        }
    )
    data = identifier + encoded_args
    return Instruction(program_id, data, keys)
