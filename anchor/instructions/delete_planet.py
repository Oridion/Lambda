from __future__ import annotations
import typing
from solders.pubkey import Pubkey
from solders.instruction import Instruction, AccountMeta
from ..program_id import PROGRAM_ID


class DeletePlanetAccounts(typing.TypedDict):
    planet: Pubkey
    universe: Pubkey
    creator: Pubkey


def delete_planet(
    accounts: DeletePlanetAccounts,
    program_id: Pubkey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> Instruction:
    keys: list[AccountMeta] = [
        AccountMeta(pubkey=accounts["planet"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["universe"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["creator"], is_signer=True, is_writable=True),
    ]
    if remaining_accounts is not None:
        keys += remaining_accounts
    identifier = b"\xe9\x1d)`\xeb\xbc\x84\xc5"
    encoded_args = b""
    data = identifier + encoded_args
    return Instruction(program_id, data, keys)
