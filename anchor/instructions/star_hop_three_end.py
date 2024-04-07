from __future__ import annotations
import typing
from solders.pubkey import Pubkey
from solders.instruction import Instruction, AccountMeta
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class StarHopThreeEndArgs(typing.TypedDict):
    deposit: int


layout = borsh.CStruct("deposit" / borsh.U64)


class StarHopThreeEndAccounts(typing.TypedDict):
    to: Pubkey
    star_one: Pubkey
    star_two: Pubkey
    star_three: Pubkey
    manager: Pubkey


def star_hop_three_end(
    args: StarHopThreeEndArgs,
    accounts: StarHopThreeEndAccounts,
    program_id: Pubkey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> Instruction:
    keys: list[AccountMeta] = [
        AccountMeta(pubkey=accounts["to"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["star_one"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["star_two"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["star_three"], is_signer=False, is_writable=True),
        AccountMeta(pubkey=accounts["manager"], is_signer=True, is_writable=True),
    ]
    if remaining_accounts is not None:
        keys += remaining_accounts
    identifier = b"\x89\x9a\xfbW\xceU\xefP"
    encoded_args = layout.build(
        {
            "deposit": args["deposit"],
        }
    )
    data = identifier + encoded_args
    return Instruction(program_id, data, keys)
