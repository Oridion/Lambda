from __future__ import annotations
import typing
from solders.pubkey import Pubkey
from solders.instruction import Instruction, AccountMeta
from ..program_id import PROGRAM_ID


class StarHopTwoEndAccounts(typing.TypedDict):
    to: Pubkey
    star_one: Pubkey
    star_two: Pubkey
    manager: Pubkey


def star_hop_two_end(
    accounts: StarHopTwoEndAccounts,
    program_id: Pubkey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> Instruction:
    keys: list[AccountMeta] = [
        AccountMeta(pubkey=accounts["to"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["star_one"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["star_two"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["manager"], is_signer=True, is_writable=True),
    ]
    if remaining_accounts is not None:
        keys += remaining_accounts
    identifier = b"j\xf4\x8a\xdfr\xf7\xf4."
    encoded_args = b""
    data = identifier + encoded_args
    return Instruction(program_id, data, keys)
